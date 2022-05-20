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

package org.apache.nifi.c2.client.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.nifi.c2.client.api.FlowUpdateInfo;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowUpdateInfoHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowUpdateInfoHolder.class);
    private static final String FLOW_IDENTIFIER_FILENAME = "flow-identifier";
    private static final String C2_FLOW_URL_KEY = "c2.flow.url";
    private static final String C2_FLOW_ID_KEY = "c2.flow.id";

    private volatile FlowUpdateInfo flowUpdateInfo;
    private final String configDirectoryName;

    public FlowUpdateInfoHolder(String configDirectoryName) {
        this.configDirectoryName = configDirectoryName;
        this.flowUpdateInfo = readFlowUpdateInfo();
    }

    public FlowUpdateInfo getFlowUpdateInfo() {
        return flowUpdateInfo;
    }

    public void setFlowUpdateInfo(FlowUpdateInfo flowUpdateInfo) {
        this.flowUpdateInfo = flowUpdateInfo;
        persistFlowUpdateInfo();
    }

    private void persistFlowUpdateInfo() {
        File flowUpdateInfoFile = new File(configDirectoryName, FLOW_IDENTIFIER_FILENAME);
        Properties props = new Properties();
        props.setProperty(C2_FLOW_URL_KEY, flowUpdateInfo.getFlowUpdateUrl());
        props.setProperty(C2_FLOW_ID_KEY, flowUpdateInfo.getFlowId());

        try {
            FileUtils.ensureDirectoryExistAndCanAccess(flowUpdateInfoFile.getParentFile());
            saveProperties(flowUpdateInfoFile, props);
        } catch (IOException e) {
            LOGGER.error("Failed to save flow information due to: {}", e.getMessage());
        }
    }

    private void saveProperties(File flowUpdateInfoFile, Properties props) {
        try (FileOutputStream fos = new FileOutputStream(flowUpdateInfoFile)) {
            props.store(fos, null);
            fos.getFD().sync();
        } catch (IOException e) {
            LOGGER.error("Failed to persist flow data", e);
        }
    }

    private FlowUpdateInfo readFlowUpdateInfo() {
        File flowUpdateInfoFile = new File(configDirectoryName, FLOW_IDENTIFIER_FILENAME);
        Properties props = new Properties();

        if (flowUpdateInfoFile.exists()) {
            try (FileInputStream fis = new FileInputStream(flowUpdateInfoFile)) {
                props.load(fis);
            } catch (IOException e) {
                LOGGER.error("Failed to load flow properties from {}", flowUpdateInfoFile.getAbsolutePath());
                LOGGER.error("Exception:", e);
            }
        }

        return Optional.ofNullable(props.getProperty(C2_FLOW_URL_KEY)).map(FlowUpdateInfo::new).orElse(null);
    }
}
