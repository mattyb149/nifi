/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.api.dto.status;

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.Collection;

/**
 * The status for a process group in this NiFi.
 */
@XmlType(name = "flowMetricsSnapshot")
public class FlowMetricsSnapshotDTO {

    private String id;
    private String name;
    private Collection<ProcessGroupStatusSnapshotEntity> processGroupStatusSnapshots;
    private Collection<BulletinEntity> bulletinEntities;

    /**
     * The id for the process group.
     *
     * @return The id for the process group
     */
    @ApiModelProperty("The id of the process group.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return name of this process group
     */
    @ApiModelProperty("The name of this process group.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The status of all process groups in this process group.
     *
     * @return The status of all process groups
     */
    @ApiModelProperty("The status of all process groups in the process group.")
    public Collection<ProcessGroupStatusSnapshotEntity> getProcessGroupStatusSnapshots() {
        return processGroupStatusSnapshots;
    }

    public void setProcessGroupStatusSnapshots(Collection<ProcessGroupStatusSnapshotEntity> processGroupStatus) {
        this.processGroupStatusSnapshots = processGroupStatus;
    }

    /**
     * The collection of all bulletins
     *
     * @return The collection of all bulletins
     */
    @ApiModelProperty("The collection of all bulletins.")
    public Collection<BulletinEntity> BulletinEntity() {
        return bulletinEntities;
    }

    public void setBulletinEntities(Collection<BulletinEntity> bulletinEntities) {
        this.bulletinEntities = bulletinEntities;
    }
}
