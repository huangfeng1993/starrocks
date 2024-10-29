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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Warehouse implements Writable {

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "state")
    protected WarehouseState state = WarehouseState.AVAILABLE;

    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "ctime")
    private volatile long createdTime;
    @SerializedName(value = "rtime")
    private volatile long resumedTime;
    @SerializedName(value = "mtime")
    private volatile long updatedTime;
    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();


    public enum WarehouseState {
        AVAILABLE,
        SUSPENDED,
    }


    private volatile boolean exist = true;

    public Warehouse(long id, String name, String comment) {
        this.id = id;
        this.name = name;
        this.comment = comment;
        this.createdTime = System.currentTimeMillis();
        this.resumedTime = -1L;
        this.updatedTime = this.createdTime;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setState(WarehouseState state) {
        this.state = state;
    }

    public WarehouseState getState() {
        return state;
    }

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    protected String getComment() {
        return this.comment;
    }

    public long getCreatedTime() {
        return this.createdTime;
    }

    public long getResumedTime() {
        return this.resumedTime;
    }

    public long getUpdatedTime() {
        return this.updatedTime;
    }

    public abstract void getProcNodeData(BaseProcResult result);

    public abstract Map<Long, Cluster> getClusters() throws DdlException;

    public abstract Cluster getAnyAvailableCluster();

    public abstract void setClusters(Map<Long, Cluster> clusters) throws DdlException;

    public abstract ProcResult getClusterProcData();

    public abstract List<List<String>> getNodesInfo();

    public abstract List<String> getWarehourseInfo();

    public abstract void initCluster() throws DdlException;

    public abstract void dropSelf() throws DdlException;

    public abstract void suspendSelf();

    public abstract void resumeSelf();


    public void setResumedTime(long time) {
        this.resumedTime = time;
    }

    public void setUpdatedTime(long time) {
        this.updatedTime = time;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Warehouse read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Warehouse.class);
    }
}
