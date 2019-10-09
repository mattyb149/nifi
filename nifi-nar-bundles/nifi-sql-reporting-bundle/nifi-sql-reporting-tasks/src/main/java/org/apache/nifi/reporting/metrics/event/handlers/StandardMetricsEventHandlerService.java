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


import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.metrics.MetricsEventHandlerService;
import org.apache.nifi.rules.Action;

import java.util.List;
import java.util.Map;

public class StandardMetricsEventHandlerService extends AbstractControllerService implements MetricsEventHandlerService {

    enum EventAction {
        ALERT, LOG, SEND, EXPRESSION;
    }

    private EventHandler logHandler;
    private EventHandler alertHandler;
    private EventHandler expressionHandler;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        logHandler = new LogHandler(getLogger());
        alertHandler = new AlertHandler(getLogger());
        expressionHandler = new ExpressionHandler(getLogger());
    }

    @Override
    public void process(final Map<String, Object> metrics, List<Action> actions) {
        if(actions == null || actions.isEmpty()){
            getLogger().warn("No actions were provided to execute");
        }else {
            actions.forEach(action -> {
                try {
                    EventAction eventAction = EventAction.valueOf(action.getType().toUpperCase());
                    sendData(metrics, eventAction, action.getAttributes());
                } catch (IllegalArgumentException iax) {
                    getLogger().warn("Action provided is not supported by this service: {}", new Object[]{iax.getMessage()}, iax);
                }
            });
        }
    }

    @Override
    public void process(final Map<String, Object> metrics, final List<Action> actions, final Map<String, EventHandler> handlerMap) {
        if(actions == null || actions.isEmpty()){
            getLogger().warn("No actions were provided to execute");
        }else {
            if (handlerMap == null) {
                process(metrics, actions);
            } else {
                actions.forEach(action -> {
                    if (handlerMap.containsKey(action.getType())) {
                        handlerMap.get(action.getType()).execute(metrics, action.getAttributes());
                    } else {
                        try {
                            EventAction eventAction = EventAction.valueOf(action.getType().toUpperCase());
                            sendData(metrics, eventAction, action.getAttributes());
                        } catch (IllegalArgumentException iax) {
                            getLogger().warn("Action provided is not supported by this service: {}", new Object[]{iax.getMessage()}, iax);
                        }
                    }
                });
            }
        }
    }

    private void sendData(final Map<String,Object> metrics, EventAction action, Map<String, String> attributes) {
        switch (action){
            case LOG:
                logHandler.execute(metrics, attributes);
                break;
            case ALERT:
                alertHandler.execute(metrics, attributes);
                break;
            case EXPRESSION:
                expressionHandler.execute(metrics, attributes);
                break;
            default:
                getLogger().warn("Provided action not available: {}", new Object[]{action.toString()});
        }
    }

}
