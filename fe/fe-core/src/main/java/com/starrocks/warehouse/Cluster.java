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

package com.starrocks.warehouse;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;

    public Cluster(long id) {
        this.id = id;
        workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
    }

    public long getId() {
        return id;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public int getRunningSqls() {
        return -1;
    }
    public int getPendingSqls() {
        return -1;
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(String.valueOf(this.getId()),
                String.valueOf(this.getWorkerGroupId()),
                String.valueOf(this.getComputeNodeIds()),
                String.valueOf(this.getPendingSqls()),
                String.valueOf(this.getRunningSqls())));
    }

    public void getProcNodesDataV2(BaseProcResult result, long warehouseId) {
        List<ComputeNode> nodes = this.getAllComputeNodes();
        for (ComputeNode node : nodes) {
            ArrayList<String> computeNodeInfo = Lists.newArrayList();
            long nodeWarehouseId = node.getWarehouseId();
            if (warehouseId != nodeWarehouseId) {
                continue;
            }
            Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getWarehouse(warehouseId);
            computeNodeInfo.add(warehouse.getName());
            computeNodeInfo.add(String.valueOf(this.id));
            computeNodeInfo.add(String.valueOf(this.workerGroupId));
            long nodeId = node.getId();
            long workerId = GlobalStateMgr.getCurrentStarOSAgent().getWorkerIdByBackendId(nodeId);
            computeNodeInfo.add(String.valueOf(nodeId));
            computeNodeInfo.add(String.valueOf(workerId));
            computeNodeInfo.add(node.getHost());
            computeNodeInfo.add(String.valueOf(node.getHeartbeatPort()));
            computeNodeInfo.add(String.valueOf(node.getBePort()));
            computeNodeInfo.add(String.valueOf(node.getHttpPort()));
            computeNodeInfo.add(String.valueOf(node.getBrpcPort()));
            computeNodeInfo.add(String.valueOf(node.getStarletPort()));
            computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastStartTime()));
            computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastUpdateMs()));
            computeNodeInfo.add(String.valueOf(node.isAlive()));
            computeNodeInfo.add(node.getHeartbeatErrMsg());
            computeNodeInfo.add(String.valueOf(node.getVersion()));
            computeNodeInfo.add(String.valueOf(node.getNumRunningQueries()));
            computeNodeInfo.add(String.valueOf(BackendCoreStat.getCoresOfBe(nodeId)));
            double memUsedPct = node.getMemUsedPct();
            computeNodeInfo.add(String.format("%.2f", memUsedPct * 100.0) + " %");
            computeNodeInfo.add(String.format("%.1f", (double) node.getCpuUsedPermille() / 10.0) + " %");
            result.addRow(computeNodeInfo);
        }
    }

    public List<Long> getComputeNodeIds() {
        if (RunMode.isSharedNothingMode()) {
            SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();
            return clusterInfoService.backendAndComputeNodeStream().map(ComputeNode::getId)
                    .collect(Collectors.toList());
        }
        List<Long> nodeIds = new ArrayList<>();
        try {
            // ask starMgr for node lists
            nodeIds = GlobalStateMgr.getCurrentStarOSAgent().
                    getWorkersByWorkerGroup(workerGroupId);
        } catch (UserException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
        }
        return nodeIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Cluster read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Cluster.class);
    }

    public void init() throws DdlException {
        try {
            StarOSAgent workGroupAgent = GlobalStateMgr.getCurrentStarOSAgent();
            this.workerGroupId = workGroupAgent.createWorkerGroup("x0");
        } catch (DdlException e) {
            LOG.warn("create Cluster {} failed, because : {}", id, e.getMessage());
            throw new DdlException("create Cluster " + id + " failed, because : " + e.getMessage());
        }
    }

    public List<ComputeNode> getAllComputeNodes() {
        SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();
        List<ComputeNode> nodes = clusterInfoService.backendAndComputeNodeStream()
                .filter(cn -> cn.getWorkerGroupId() == this.getWorkerGroupId())
                .collect(Collectors.toList());
        return nodes;
    }

}
