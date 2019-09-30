package org.apache.nifi.diagnostics.event.handlers;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.diagnostics.DiagnosticEventHandlerService;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockDiagnosticEventHandlerService  extends AbstractConfigurableComponent implements DiagnosticEventHandlerService {

    private List<Map<String, Object>> rows = new ArrayList<>();

    @Override
    public void sendData(Map<String, Object> metrics, EventAction action, Map<String, String> attributes) {

        if(metrics != null && !metrics.isEmpty()) {
            rows.add(metrics);
        }
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    @Override
    public String getIdentifier() {
        return "MockDiagnosticEventHandlerService";
    }
}
