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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CMD_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.OK_STATUS_CODE;

import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.MiNiFiStatus;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStatusProvider;

public class StatusService implements CommandService{
    private final MiNiFiParameters miNiFiParameters;
    private final MiNiFiStatusProvider miNiFiStatusProvider;

    public StatusService(MiNiFiParameters miNiFiParameters, MiNiFiStatusProvider miNiFiStatusProvider) {
        this.miNiFiParameters = miNiFiParameters;
        this.miNiFiStatusProvider = miNiFiStatusProvider;
    }

    @Override
    public int runCommand(String[] args) {
        return status();
    }

    private int status() {
        MiNiFiStatus status = miNiFiStatusProvider.getStatus(miNiFiParameters.getMiNiFiPort(), miNiFiParameters.getMinifiPid());
        if (status.isRespondingToPing()) {
            CMD_LOGGER.info("Apache MiNiFi is currently running, listening to Bootstrap on port {}, PID={}",
                status.getPort(), status.getPid() == null ? "unknown" : status.getPid());
            return OK_STATUS_CODE;
        }

        if (status.isProcessRunning()) {
            CMD_LOGGER.info("Apache MiNiFi is running at PID {} but is not responding to ping requests", status.getPid());
            return 4;
        }

        if (status.getPort() == null) {
            CMD_LOGGER.info("Apache MiNiFi is not running");
            return 3;
        }

        if (status.getPid() == null) {
            CMD_LOGGER.info("Apache MiNiFi is not responding to Ping requests. The process may have died or may be hung");
        } else {
            CMD_LOGGER.info("Apache MiNiFi is not running");
        }
        return 3;
    }
}
