package org.apache.nifi.reporting.diagnostics.event.handlers;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.reporting.diagnostics.DiagnosticEventHandlerService;

import java.util.*;

public class StandardDiagnosticEventHandlerService extends AbstractControllerService implements DiagnosticEventHandlerService {

    @Override
    public void sendData(final Map<String,Object> metrics, EventAction action, Map<String, String> attributes) {
        if(action.equals(EventAction.LOG)){
            log(metrics,attributes);
        }else{
            getLogger().warn("Provided action not available: {}", new Object[]{action.toString()});
        }
    }

    protected void log(Map<String,Object> metrics, Map<String, String> attributes){

        final String logLevel = StringUtils.isNotEmpty(attributes.get("logLevel")) ?
                                attributes.get("logLevel").toLowerCase() : "info ";

        final String eventMessage = StringUtils.isNotEmpty(attributes.get("message")) ?
                attributes.get("message") : "Event triggered log.";

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
            getLogger().log(LogLevel.valueOf(logLevel.toUpperCase()),outputMessage);
        } catch (IllegalArgumentException iax){
            getLogger().info(outputMessage);
        }

    }


}
