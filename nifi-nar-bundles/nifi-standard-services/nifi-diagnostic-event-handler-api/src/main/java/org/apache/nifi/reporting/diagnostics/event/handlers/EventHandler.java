package org.apache.nifi.reporting.diagnostics.event.handlers;

import java.util.Map;

public interface EventHandler {

    void execute(final Map<String,Object> metrics,final Map<String,String> attributes);

}
