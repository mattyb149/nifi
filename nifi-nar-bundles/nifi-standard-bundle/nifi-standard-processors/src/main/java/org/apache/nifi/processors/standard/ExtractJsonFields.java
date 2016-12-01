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
package org.apache.nifi.processors.standard;

import io.advantageous.boon.json.JsonParser;
import io.advantageous.boon.json.JsonParserFactory;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "evaluate", "JsonPath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates one or more simple JsonPath expressions against the content of a FlowFile. "
        + "The results of those expressions are assigned to FlowFile Attributes or are written to the content of the FlowFile itself, "
        + "depending on configuration of the Processor. "
        + "JsonPaths are entered by adding user-defined properties; the name of the property maps to the Attribute Name "
        + "into which the result will be placed (if the Destination is flowfile-attribute; otherwise, the property name is ignored). "
        + "The value of the property must be a valid JsonPath expression of the form $.field[i], where 'field' is a field name and '[i]' is an optional index "
        + "(with integer i) for indexing into an array. The JSON document must be 'flat' in the sense that it contains scalar values or arrays of scalar values. "
        + "If Destination is 'flowfile-content' and the JsonPath does not evaluate to a defined path, the FlowFile will be routed to 'unmatched' without having its contents modified. "
        + "If Destination is flowfile-attribute and the expression matches nothing, attributes will be created with "
        + "empty strings as the value, and the FlowFile will always be routed to 'matched.'")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute')",
        value = "A JsonPath expression", description = "If <Destination>='flowfile-attribute' then that FlowFile attribute "
        + "will be set to any JSON objects that match the JsonPath.  If <Destination>='flowfile-content' then the FlowFile "
        + "content will be updated to any JSON objects that match the JsonPath.")
public class ExtractJsonFields extends AbstractProcessor {

    private static final Map<String, String> NULL_REPRESENTATION_MAP = new HashMap<>();
    private static final String EMPTY_STRING_OPTION = "empty string";
    private static final String NULL_STRING_OPTION = "the string 'null'";

    static {
        NULL_REPRESENTATION_MAP.put(EMPTY_STRING_OPTION, "");
        NULL_REPRESENTATION_MAP.put(NULL_STRING_OPTION, "null");
    }

    private static final Pattern SIMPLE_JSONPATH_REGEX = Pattern.compile("\\$\\.([a-zA-Z][a-zA-Z0-9]*)(\\[([0-9]+)\\])?");
    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String PATH_NOT_FOUND_IGNORE = "ignore";
    public static final String PATH_NOT_FOUND_WARN = "warn";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the JsonPath evaluation are written to the FlowFile content or a FlowFile attribute; "
                    + "if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one JsonPath may be specified, "
                    + "and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor PATH_NOT_FOUND = new PropertyDescriptor.Builder()
            .name("Path Not Found Behavior")
            .description("Indicates how to handle missing JSON path expressions when destination is set to 'flowfile-attribute'. Selecting 'warn' will "
                    + "generate a warning when a JSON path expression is not found.")
            .required(true)
            .allowableValues(PATH_NOT_FOUND_WARN, PATH_NOT_FOUND_IGNORE)
            .defaultValue(PATH_NOT_FOUND_IGNORE)
            .build();

    public static final PropertyDescriptor NULL_VALUE_DEFAULT_REPRESENTATION = new PropertyDescriptor.Builder()
            .name("Null Value Representation")
            .description("Indicates the desired representation of JSON Path expressions resulting in a null value.")
            .required(true)
            .allowableValues(NULL_REPRESENTATION_MAP.keySet())
            .defaultValue(EMPTY_STRING_OPTION)
            .build();


    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the JsonPath is successfully evaluated and the FlowFile is modified as a result")
            .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when the JsonPath does not match the content of the FlowFile and the Destination is set to flowfile-content")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when the JsonPath cannot be evaluated against the content of the "
                    + "FlowFile; for instance, if the FlowFile is not valid JSON")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final Map<String, PathHolder> attributeToJsonPathMap = new HashMap<>();
    private volatile String destination = null;
    private volatile String nullDefaultValue = null;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(PATH_NOT_FOUND);
        properties.add(NULL_VALUE_DEFAULT_REPRESENTATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            final int jsonPathCount = (int) context.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic).count();

            if (jsonPathCount != 1) {
                results.add(new ValidationResult.Builder().subject("JsonPaths").valid(false)
                        .explanation("Exactly one JsonPath must be set if using destination of " + DESTINATION_CONTENT).build());
            }
        }

        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName).expressionLanguageSupported(false)
                .addValidator(StandardValidators.createRegexMatchingValidator(SIMPLE_JSONPATH_REGEX))
                .required(false).dynamic(true).build();
    }

    @OnScheduled
    public void setup(final ProcessContext processContext) {

        String representationOption = processContext.getProperty(NULL_VALUE_DEFAULT_REPRESENTATION).getValue();
        nullDefaultValue = NULL_REPRESENTATION_MAP.get(representationOption);

        /* Build the JsonPath expressions from attributes */
        for (final Map.Entry<PropertyDescriptor, String> entry : processContext.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            Matcher m = SIMPLE_JSONPATH_REGEX.matcher(entry.getValue());
            if (m.matches()) {
                String fieldName = m.group(1);
                String index = m.group(3);

                attributeToJsonPathMap.put(entry.getKey().getName(),
                        new PathHolder(fieldName, index == null ? null : Integer.parseInt(index), entry.getValue()));
            }
        }

        destination = processContext.getProperty(DESTINATION).getValue();
    }


    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final String pathNotFound = processContext.getProperty(PATH_NOT_FOUND).getValue();

        Map fieldMap;

        try {
            JsonParser parser = new JsonParserFactory().create();
            // Assume a top-level JSON object (vs an array)
            fieldMap = (Map) parser.parse(new BufferedInputStream(processSession.read(flowFile)));

        } catch (Exception e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile}, e);
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributeResults = new HashMap<>();

        for (final Map.Entry<String, PathHolder> attributeJsonPathEntry : attributeToJsonPathMap.entrySet()) {

            final String jsonPathAttrKey = attributeJsonPathEntry.getKey();
            final PathHolder jsonPathExp = attributeJsonPathEntry.getValue();
            final String jsonPath = jsonPathExp.getJsonPath();
            final Integer index = jsonPathExp.getIndex();
            String jsonPathResult = null;

            Object o = fieldMap.get(jsonPathExp.getFieldName());
            if (o != null) {
                if (o instanceof List) {
                    if (index == null) {
                        logger.error("Unable to return a scalar value for the expression {} for FlowFile {}, the field is an array but the expression has no index. Transferring to {}.",
                                new Object[]{jsonPath, flowFile.getId(), REL_FAILURE.getName()});
                        processSession.transfer(flowFile, REL_FAILURE);
                        return;
                    } else {
                        List a = ((List) o);
                        if (index >= a.size() || index < 0) {
                            logger.error("Unable to return a scalar value for the expression {} for FlowFile {}, the array index is out of bounds. Transferring to {}.",
                                    new Object[]{jsonPath, flowFile.getId(), REL_FAILURE.getName()});
                            processSession.transfer(flowFile, REL_FAILURE);
                            return;
                        } else {
                            jsonPathResult = a.get(index).toString();
                        }
                    }
                } else {
                    jsonPathResult = o.toString();
                }

            } else if (pathNotFound.equals(PATH_NOT_FOUND_WARN)) {
                logger.warn("FlowFile {} could not find path {} for attribute key {}.",
                        new Object[]{flowFile.getId(), jsonPath, jsonPathAttrKey});

                if (destination.equals(DESTINATION_ATTRIBUTE)) {
                    attributeResults.put(jsonPathAttrKey, StringUtils.EMPTY);
                    continue;
                } else {
                    processSession.transfer(flowFile, REL_NO_MATCH);
                    return;
                }
            }

            switch (destination) {
                case DESTINATION_ATTRIBUTE:
                    attributeResults.put(jsonPathAttrKey, jsonPathResult);
                    break;
                case DESTINATION_CONTENT:
                    final byte[] b = (jsonPathResult == null ? nullDefaultValue : jsonPathResult).getBytes(StandardCharsets.UTF_8);
                    flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(b);
                            }
                        }
                    });
                    processSession.getProvenanceReporter().modifyContent(flowFile, "Replaced content with result of expression " + jsonPath);
                    break;
            }
        }

        flowFile = processSession.putAllAttributes(flowFile, attributeResults);
        processSession.transfer(flowFile, REL_MATCH);
    }

    private static class PathHolder {

        private String fieldName;
        private Integer index;
        private String jsonPath;

        public String getJsonPath() {
            return jsonPath;
        }

        public PathHolder(String fieldName, Integer index, String jsonPath) {
            this.fieldName = fieldName;
            this.index = index;
            this.jsonPath = jsonPath;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Integer getIndex() {
            return index;
        }
    }
}
