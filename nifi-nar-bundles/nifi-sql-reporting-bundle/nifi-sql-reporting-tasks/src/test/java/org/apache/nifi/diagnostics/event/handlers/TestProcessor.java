package org.apache.nifi.diagnostics.event.handlers;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.diagnostics.MetricsEventHandlerService;

import java.util.ArrayList;
import java.util.List;

public class TestProcessor extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptor.Builder()
                .name("diagnostic-event-handler-service-test")
                .description("Diagnostic event handler service")
                .identifiesControllerService(MetricsEventHandlerService.class)
                .required(true)
                .build());
        return properties;
    }

}
