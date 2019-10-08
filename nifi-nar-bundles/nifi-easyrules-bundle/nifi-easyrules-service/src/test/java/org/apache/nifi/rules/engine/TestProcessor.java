package org.apache.nifi.rules.engine;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

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
                    .name("easy-rules-engine-service-test")
                    .description("Easy Rules Engine Service")
                    .identifiesControllerService(EasyRulesEngineService.class)
                    .required(true)
                    .build());
            return properties;
        }

}
