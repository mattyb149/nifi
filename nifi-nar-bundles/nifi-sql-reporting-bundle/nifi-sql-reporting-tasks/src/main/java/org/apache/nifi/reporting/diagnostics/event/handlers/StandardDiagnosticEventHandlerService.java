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
package org.apache.nifi.reporting.diagnostics.event.handlers;


import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.diagnostics.DiagnosticEventHandlerService;

import java.util.Map;



public class StandardDiagnosticEventHandlerService extends AbstractControllerService implements DiagnosticEventHandlerService {

    private EventHandler logHandler;
    private EventHandler alertHandler;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        logHandler = new LogHandler(getLogger());
        alertHandler = new AlertHandler(getLogger());
    }

    @Override
    public void sendData(Map<String, Object> metrics, EventAction action, Map<String, String> attributes, Map<EventAction, EventHandler> handlerMap) {
        if(handlerMap == null || !handlerMap.containsKey(action)){
            sendData(metrics,action,attributes);
        }else{
            handlerMap.get(action).execute(metrics, attributes);
        }
    }

    @Override
    public void sendData(final Map<String,Object> metrics, EventAction action, Map<String, String> attributes) {
        switch (action){
            case LOG:
                logHandler.execute(metrics, attributes);
                break;
            case ALERT:
                alertHandler.execute(metrics, attributes);
                break;
            default:
                getLogger().warn("Provided action not available: {}", new Object[]{action.toString()});
        }
    }

}
