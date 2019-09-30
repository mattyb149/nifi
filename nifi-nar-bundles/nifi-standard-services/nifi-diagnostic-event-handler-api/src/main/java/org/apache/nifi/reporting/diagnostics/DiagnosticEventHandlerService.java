package org.apache.nifi.reporting.diagnostics;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.Record;

import java.util.Map;

public interface DiagnosticEventHandlerService extends ControllerService {

    enum EventAction{
        LOG;
    }

    void sendData(final Map<String,Object> metrics, EventAction action, final Map<String,String> attributes);

}
