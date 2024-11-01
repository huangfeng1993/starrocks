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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;
import java.util.Map;

// on-premise
public class LocalWarehouse extends Warehouse {
    @SerializedName(value = "cluster")
    Cluster cluster;

    public static final long DEFAULT_CLUSTER_ID = 0L;

    public static final ImmutableList<String> CLUSTER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ClusterId")
            .add("WorkerGroupId")
            .add("ComputeNodeIds")
            .add("Pending")
            .add("Running")
            .build();

    public LocalWarehouse(long id, String name) {
        super(id, name, "");
        cluster = new Cluster(DEFAULT_CLUSTER_ID);
    }

    public LocalWarehouse(long id, String name, long clusterId, String comment) {
        super(id, name, comment);
        cluster = new Cluster(clusterId);
    }

    @Override
    public void getProcNodeData(BaseProcResult result) {
        result.addRow(getWarehourseInfo());
    }

    @Override
    public List<String> getWarehourseInfo() {
        return Lists.newArrayList(String.valueOf(this.getId()),
                this.getName(),
                this.getState().toString(),
                String.valueOf(cluster.getAllComputeNodes().size()),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(0L),
                String.valueOf(0L),
                TimeUtils.longToTimeString(this.getCreatedTime()),
                TimeUtils.longToTimeString(this.getResumedTime()),
                TimeUtils.longToTimeString(this.getUpdatedTime()),
                this.getComment());
    }

    @Override
    public Map<Long, Cluster> getClusters() throws DdlException {
        return ImmutableMap.of(cluster.getId(), cluster);
    }

    @Override
    public void setClusters(Map<Long, Cluster> clusters) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public Cluster getAnyAvailableCluster() {
        return cluster;
    }

    @Override
    public ProcResult getClusterProcData() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(CLUSTER_PROC_NODE_TITLE_NAMES);
        cluster.getProcNodeData(result);
        return result;
    }

    @Override
    public List<List<String>> getNodesInfo() {
        BaseProcResult result = new BaseProcResult();
        cluster.getProcNodesDataV2(result, getId());
        return result.getRows();
    }

    @Override
    public void initCluster() throws DdlException {
        cluster.init();
    }

    @Override
    public void dropSelf() throws DdlException {
        this.deleteWorkerFromStarMgr();
        this.dropNodeFromSystem();
    }

    @Override
    public void suspendSelf() {
        this.state = Warehouse.WarehouseState.SUSPENDED;
        long currentTime = System.currentTimeMillis();
        this.setResumedTime(currentTime);
        this.setUpdatedTime(currentTime);
    }

    @Override
    public void resumeSelf() {
        this.state = Warehouse.WarehouseState.AVAILABLE;
        long currentTime = System.currentTimeMillis();
        this.setUpdatedTime(currentTime);
    }

    private void deleteWorkerFromStarMgr() throws DdlException {
        long workerGroupId = this.cluster.getWorkerGroupId();
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentStarOSAgent();
        starOSAgent.deleteWorkerGroup(workerGroupId);
    }

    private void dropNodeFromSystem() throws DdlException {
        GlobalStateMgr.getCurrentSystemInfo().dropNodes(this.getId());
    }
}
