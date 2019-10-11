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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

import java.util.Map;
import java.util.Set;

public class LogHandler extends AbstractEventHandler{

    public LogHandler(ComponentLog logger) {
        super(logger);
    }

    @Override
    public void execute(Map<String, Object> metrics, Map<String, String> attributes) {
        log(metrics, attributes);
    }

    protected void log(Map<String,Object> metrics, Map<String, String> attributes){
        final String logLevel = attributes.get("logLevel");
        final LogLevel level = getLogLevel(logLevel, LogLevel.INFO);
        final String eventMessage = StringUtils.isNotEmpty(attributes.get("message")) ? attributes.get("message") : "Event triggered log.";
        logMessage(metrics, level, eventMessage);
    }

    protected LogLevel getLogLevel(String logLevel, LogLevel defaultLevel){
        LogLevel level;
        if (StringUtils.isNotEmpty(logLevel)){
            try{
                level = LogLevel.valueOf(logLevel.toUpperCase());
            }catch (IllegalArgumentException iea){
                level = defaultLevel;
            }
        }else{
            level = defaultLevel;
        }
        return level;
    }

    protected void logMessage(Map<String,Object> metrics, LogLevel logLevel, String eventMessage){

        final Set<String> fields = metrics.keySet();
        final StringBuilder message = new StringBuilder();
        final String dashedLine = StringUtils.repeat('-', 50);

        message.append("\n");
        message.append(dashedLine);
        message.append("\n");
        message.append("Event Message: ");
        message.append(eventMessage);
        message.append("\n");
        message.append("Event Metrics:\n");

        fields.forEach( field ->{
            message.append("Field: ");
            message.append(field);
            message.append(", Value: ");
            message.append(metrics.get(field));
            message.append("\n");
        });

        String outputMessage = message.toString().trim();

        try {
            logger.log(logLevel,outputMessage);
        } catch (IllegalArgumentException iax){
            logger.info(outputMessage);
        }
    }


}
