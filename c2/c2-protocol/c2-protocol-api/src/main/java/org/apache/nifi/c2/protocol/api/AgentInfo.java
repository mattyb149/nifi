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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;

import java.io.Serializable;

@ApiModel
public class AgentInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String identifier;
    private String agentClass;
    private RuntimeManifest runtimeManifest;
    private AgentStatus status;

    @ApiModelProperty(
            value = "A unique identifier for the Agent",
            notes = "Usually set when the agent is provisioned and deployed",
            required = true)
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty(
            value = "The class or category label of the agent, e.g., 'sensor-collector'",
            notes = "Usually set when the agent is provisioned and deployed")
    public String getAgentClass() {
        return agentClass;
    }

    public void setAgentClass(String agentClass) {
        this.agentClass = agentClass;
    }

    @ApiModelProperty("The specification of the agent's capabilities")
    public RuntimeManifest getAgentManifest() {
        return runtimeManifest;
    }

    public void setAgentManifest(RuntimeManifest runtimeManifest) {
        this.runtimeManifest = runtimeManifest;
    }

    @ApiModelProperty("A summary of the runtime status of the agent")
    public AgentStatus getStatus() {
        return status;
    }

    public void setStatus(AgentStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "AgentInfo{" +
                "identifier='" + identifier + '\'' +
                ", agentClass='" + agentClass + '\'' +
                '}';
    }
}
