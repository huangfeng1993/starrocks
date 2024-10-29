// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.server;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseProcDir;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WarehouseManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(WarehouseManager.class);

    public static final String DEFAULT_WAREHOUSE_NAME = "default_warehouse";

    public static final long DEFAULT_WAREHOUSE_ID = 0L;

    private final Map<Long, Warehouse> idToWh = new HashMap<>();
    private final Map<String, Warehouse> nameToWh = new HashMap<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WarehouseManager() {
    }

    public void initDefaultWarehouse() {
        // gen a default warehouse
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Warehouse wh = new LocalWarehouse(DEFAULT_WAREHOUSE_ID,
                    DEFAULT_WAREHOUSE_NAME);
            nameToWh.put(wh.getName(), wh);
            idToWh.put(wh.getId(), wh);
            wh.setExist(true);
        }
    }

    public Warehouse getDefaultWarehouse() {
        return getWarehouse(DEFAULT_WAREHOUSE_NAME);
    }

    public List<Warehouse> getAllWarehouses() {
        return new ArrayList<>(nameToWh.values());
    }

    public Warehouse getWarehouse(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.get(warehouseName);
        }
    }

    public Warehouse getWarehouse(long warehouseId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return idToWh.get(warehouseId);
        }
    }

    public List<Long> getWarehouseIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new ArrayList<>(idToWh.keySet());
        }
    }

    public boolean warehouseExists(String warehouseName) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return nameToWh.containsKey(warehouseName);
        }
    }

    public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse() {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        Warehouse warehouse = getDefaultWarehouse();
        warehouse.getAnyAvailableCluster().getComputeNodeIds().forEach(
                nodeId -> builder.put(nodeId, GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId)));
        return builder.build();
    }

    //create warehouse
    public void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }
        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            if (this.nameToWh.containsKey(warehouseName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("Warehouse '{}' already exists", warehouseName);
                    return;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_EXISTS, warehouseName);
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            long clusterId = GlobalStateMgr.getCurrentState().getNextId();
            String comment = stmt.getComment();
            LocalWarehouse wh = new LocalWarehouse(id, warehouseName, clusterId, comment);
            try {
                wh.initCluster();
            } catch (DdlException e) {
                LOG.warn("create warehouse {} failed, reason: {}", wh.getName(), e.getMessage());
                throw new DdlException("create warehouse " + wh.getName() + " failed, reason: " + e.getMessage());
            }
            this.nameToWh.put(wh.getName(), wh);
            this.idToWh.put(wh.getId(), wh);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateWarehouse(wh);
            LOG.info("createWarehouse whName = {}, id = {}, comment = {}", warehouseName, id, comment);
        }
    }

    //drop warehouse
    public void dropWarehouse(DropWarehouseStmt stmt) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }
        String warehouseName = stmt.getWarehouseName();
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            Warehouse warehouse = this.nameToWh.get(warehouseName);
            if (warehouse == null) {
                if (stmt.isSetIfExists()) {
                    return;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
            }
            this.nameToWh.remove(warehouseName);
            this.idToWh.remove(warehouse.getId());
            warehouse.dropSelf();
            GlobalStateMgr.getCurrentState().getEditLog().logDropWarehouse(warehouseName);
        }
    }

    public void suspend(String warehouseName) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            Preconditions.checkState(this.nameToWh.containsKey(warehouseName), "Warehouse '%s' doesn't exist",
                    warehouseName);
            Warehouse warehouse = this.nameToWh.get(warehouseName);
            if (warehouse.getState() == Warehouse.WarehouseState.SUSPENDED) {
                ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_SUSPENDED, warehouseName);
            }
            warehouse.suspendSelf();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterWarehouse(warehouse);
        }
    }

    public void resume(String warehouseName) throws DdlException {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            Preconditions.checkState(this.nameToWh.containsKey(warehouseName), "Warehouse '%s' doesn't exist",
                    warehouseName);
            Warehouse warehouse = this.nameToWh.get(warehouseName);
            warehouse.resumeSelf();
            GlobalStateMgr.getCurrentState().getEditLog().logAlterWarehouse(warehouse);
        }
    }

    public void replayCreateWarehouse(Warehouse warehouse) {
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            this.nameToWh.put(warehouse.getName(), warehouse);
            this.idToWh.put(warehouse.getId(), warehouse);
        }
    }

    public void replayDropWarehouse(DropWarehouseLog log) {
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            String warehouseName = log.getWarehouseName();
            if (this.nameToWh.containsKey(warehouseName)) {
                Warehouse warehouse = this.nameToWh.remove(warehouseName);
                this.idToWh.remove(warehouse.getId());
            }
        }
    }

    public void replayAlterWarehouse(Warehouse warehouse) {
        try (LockCloseable lock = new LockCloseable(this.rwLock.writeLock())) {
            this.nameToWh.put(warehouse.getName(), warehouse);
            this.idToWh.put(warehouse.getId(), warehouse);
        }
    }

    // not persist anything thereafter, so checksum ^= 0
    public long saveWarehouses(DataOutputStream out, long checksum) throws IOException {
        checksum ^= 0;
        write(out);
        return checksum;
    }

    // persist warehouse info
    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.WAREHOUSE_MGR, this.nameToWh.size() + 1);
        writer.writeJson(nameToWh.size());
        for (Warehouse warehouse : nameToWh.values()) {
            writer.writeJson(warehouse);
        }
        writer.close();
    }

    public List<List<String>> getWarehousesInfo() {
        return new WarehouseProcDir(this).fetchResult().getRows();
    }

    public long loadWarehouses(DataInputStream dis, long checksum) throws IOException, DdlException {
        int warehouseCount = 0;
        try {
            String s = Text.readString(dis);
            WarehouseManager data = GsonUtils.GSON.fromJson(s, WarehouseManager.class);
            if (data != null && data.nameToWh != null) {
                warehouseCount = data.nameToWh.size();
            }
            checksum ^= warehouseCount;
            LOG.info("finished replaying WarehouseMgr from image");
        } catch (EOFException e) {
            LOG.info("no WarehouseMgr to replay.");
        }
        return checksum;
    }

    public void load(SRMetaBlockReader reader) throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        int nameToWhSize = reader.readJson(Integer.TYPE);
        for (int i = 0; i != nameToWhSize; ++i) {
            Warehouse warehouse = reader.readJson(Warehouse.class);
            nameToWh.put(warehouse.getName(), warehouse);
            idToWh.put(warehouse.getId(), warehouse);
            LOG.info("warehouse {} success to load data from persist storage, info: {}", warehouse.getId(),
                    warehouse.toString());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public Warehouse getAvailbleWarehouse(long warehouseId) throws UserException {
        Warehouse warehouse = getWarehouse(warehouseId);
        if (warehouse == null) {
            ErrorReport.reportWarehouseUnavailableException(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.valueOf(warehouseId));
        }
        if (warehouse.getState() == Warehouse.WarehouseState.SUSPENDED) {
            ErrorReport.reportWarehouseUnavailableException(ErrorCode.ERR_WAREHOUSE_SUSPENDED, warehouse.getName());
        }
        return warehouse;
    }

    public Warehouse getAvailbleWarehouse(String warehouseName) throws UserException {
        Warehouse warehouse = this.getWarehouse(warehouseName);
        if (warehouse == null) {
            ErrorReport.reportWarehouseUnavailableException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
        }
        if (warehouse.getState() == Warehouse.WarehouseState.SUSPENDED) {
            ErrorReport.reportWarehouseUnavailableException(ErrorCode.ERR_WAREHOUSE_SUSPENDED, warehouseName);
        }
        return warehouse;
    }

    public ImmutableMap<Long, ComputeNode> getComputeNodesFromAvailableWarehouse(long warehouseId) throws
            UserException {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        Warehouse warehouse = getAvailbleWarehouse(warehouseId);
        warehouse.getAnyAvailableCluster()
                .getComputeNodeIds()
                .stream()
                .forEach(nodeId -> builder.put(nodeId,
                        GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId)));
        return builder.build();

    }
}