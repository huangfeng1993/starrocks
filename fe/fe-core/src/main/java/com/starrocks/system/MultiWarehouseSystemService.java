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

package com.starrocks.system;

import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.warehouse.Warehouse;

import java.util.List;
import java.util.stream.Collectors;

public class MultiWarehouseSystemService extends SystemInfoService {

    public MultiWarehouseSystemService() {
    }

    public MultiWarehouseSystemService(SystemInfoService systemInfo) {
        this.idToComputeNodeRef = systemInfo.idToComputeNodeRef;
        this.idToBackendRef = systemInfo.idToBackendRef;
    }

    @Override
    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs, String warehouseName) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            checkSameNodeExist(pair.first, pair.second);
        }
        for (Pair<String, Integer> pair : hostPortPairs) {
            this.addComputeNode(pair.first, pair.second, warehouseName);
        }
    }

    private void addComputeNode(String host, Integer heartbeatPort, String warehouseName) throws DdlException {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        this.setComputeNodeOwner(newComputeNode);
        this.addComputeNode(newComputeNode, warehouseName);

        this.idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);
        GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
        LOG.info("finished to add {} ", newComputeNode);
    }

    public void addComputeNode(ComputeNode computeNode, String warehouseName) throws DdlException {
        Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getWarehouse(warehouseName);
        if (warehouse == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
        }
        computeNode.setWorkerGroupId(warehouse.getAnyAvailableCluster().getWorkerGroupId());
        computeNode.setWarehouseId(warehouse.getId());
    }

    public void dropNodes(long warehouseId) throws DdlException {
        List<ComputeNode> nodes =
                this.backendAndComputeNodeStream().filter(cn -> cn.getWarehouseId() == warehouseId).collect(
                        Collectors.toList());
        for (ComputeNode node : nodes) {
            try {
                if (node instanceof Backend) {
                    this.dropBackend(node.getHost(), node.getHeartbeatPort(), false);
                    continue;
                }
                this.dropComputeNode(node.getHost(), node.getHeartbeatPort());
            } catch (DdlException e) {
                if (e.getMessage().contains("compute node does not exists") ||
                        e.getMessage().contains("backend does not exists")) {
                    continue;
                }
                throw e;
            }
        }
    }

    @Override
    public void dropComputeNode(String host, int heartbeatPort) throws DdlException {
        long starletPort;
        ComputeNode dropComputeNode = this.getComputeNodeWithHeartbeatPort(host, heartbeatPort);
        if (dropComputeNode == null) {
            throw new DdlException("compute node does not exists[" + host + ":" + heartbeatPort + "]");
        }
        if (RunMode.isSharedDataMode() && (starletPort = dropComputeNode.getStarletPort()) != 0L) {
            String workerAddr = dropComputeNode.getHost() + ":" + starletPort;
            GlobalStateMgr.getCurrentStarOSAgent().removeWorker(workerAddr, dropComputeNode.getWorkerGroupId());
        }
        this.idToComputeNodeRef.remove(dropComputeNode.getId());
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDropComputeNode(new DropComputeNodeLog(dropComputeNode.getId()));
        LOG.info("finished to drop {}", dropComputeNode);
    }

    public void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs, String warehouseName) throws DdlException {
        if (GlobalStateMgr.getCurrentWarehouseMgr().getWarehouse(warehouseName) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, warehouseName);
        }
        for (Pair<String, Integer> pair : hostPortPairs) {
            ComputeNode cn = this.getComputeNodeWithHeartbeatPort(pair.first, pair.second);
            if (cn == null) {
                throw new DdlException("compute node does not exists[" + pair.first + ":" + pair.second + "]");
            }
            Warehouse wh = GlobalStateMgr.getCurrentWarehouseMgr().getWarehouse(cn.getWarehouseId());
            if (wh != null && !warehouseName.equalsIgnoreCase(wh.getName())) {
                throw new DdlException(
                        "compute node [" + pair.first + ":" + pair.second + "] does not exist in warehouse " +
                                warehouseName);
            }
        }
        for (Pair<String, Integer> pair : hostPortPairs) {
            this.dropComputeNode(pair.first, pair.second);
        }
    }

}
