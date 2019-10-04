package org.apache.nifi.reporting.diagnostics.event.handlers;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

import java.util.Map;

public class AlertHandler extends LogHandler {

    public AlertHandler(ComponentLog logger) {
        super(logger);
    }

    @Override
    protected void log(Map<String, Object> metrics, Map<String, String> attributes) {
        alert(metrics, attributes);
    }

    protected void alert(Map<String,Object> metrics, Map<String, String> attributes){
        final String logLevel = attributes.get("logLevel");
        final LogLevel level = getLogLevel(logLevel, LogLevel.WARN);
        final String eventMessage = StringUtils.isNotEmpty(attributes.get("message")) ? attributes.get("message") : "Event triggered log.";
        logMessage(metrics, level, eventMessage);
    }
}
