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
package org.apache.nifi.reporting.metrics.event.handlers;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.metrics.MetricsEventHandlerService;
import org.apache.nifi.reporting.metrics.event.handlers.EventHandler;
import org.apache.nifi.rules.Action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockMetricsEventHandlerService extends AbstractConfigurableComponent implements MetricsEventHandlerService {

    private List<Map<String, Object>> rows = new ArrayList<>();
    private List<String> defaultActions = new ArrayList<>();
    private List<String> customActions = new ArrayList<>();


    @Override
    public void process(Map<String, Object> metrics, List<Action> actions) {
        rows.add(metrics);
        if(actions != null && !actions.isEmpty()){
            actions.forEach(action -> {
                defaultActions.add(action.getType());
            });
        }
    }

    @Override
    public void process(Map<String, Object> metrics, List<Action> actions, Map<String, EventHandler> handlerMap) {
        rows.add(metrics);
        if(actions != null && !actions.isEmpty()){
            actions.forEach(action -> {
                if(handlerMap != null && !handlerMap.isEmpty()){
                    if (handlerMap.containsKey(action.getType())) {
                        customActions.add(action.getType());
                    } else {
                        defaultActions.add(action.getType());
                    }
                }
            });
        }
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public List<String> getDefaultActions() {
        return defaultActions;
    }

    public List<String> getCustomActions() {
        return customActions;
    }

    @Override
    public String getIdentifier() {
        return "MockDiagnosticEventHandlerService";
    }
}
