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

import org.antlr.v4.runtime.misc.ParseCancellationException;
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
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.standard.util.JsonPathExpressionValidator;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.StringUtils;
import org.jsfr.json.JsonPathListener;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.ParsingContext;
import org.jsfr.json.SurfingConfiguration;
import org.jsfr.json.exception.JsonSurfingException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.standard.AbstractJsonPathProcessor.getResultRepresentation;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "evaluate", "JsonPath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates one or more JsonPath expressions against the content of a FlowFile. "
        + "The results of those expressions are assigned to FlowFile Attributes or are written to the content of the FlowFile itself, "
        + "depending on configuration of the Processor. "
        + "JsonPaths are entered by adding user-defined properties; the name of the property maps to the Attribute Name "
        + "into which the result will be placed (if the Destination is flowfile-attribute; otherwise, the property name is ignored). "
        + "The value of the property must be a valid JsonPath expression. "
        + "A Return Type of 'auto-detect' will make a determination based off the configured destination. "
        + "When 'Destination' is set to 'flowfile-attribute,' a return type of 'scalar' will be used. "
        + "When 'Destination' is set to 'flowfile-content,' a return type of 'JSON' will be used."
        + "If the JsonPath evaluates to a JSON array or JSON object and the Return Type is set to 'scalar' the FlowFile will be unmodified and will be routed to failure. "
        + "A Return Type of JSON can return scalar values if the provided JsonPath evaluates to the specified value and will be routed as a match."
        + "If Destination is 'flowfile-content' and the JsonPath does not evaluate to a defined path, the FlowFile will be routed to 'unmatched' without having its contents modified. "
        + "If Destination is flowfile-attribute and the expression matches nothing, attributes will be created with "
        + "empty strings as the value, and the FlowFile will always be routed to 'matched.'")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute')",
        value = "A JsonPath expression", description = "If <Destination>='flowfile-attribute' then that FlowFile attribute "
        + "will be set to any JSON objects that match the JsonPath.  If <Destination>='flowfile-content' then the FlowFile "
        + "content will be updated to any JSON objects that match the JsonPath.")
public class ExtractJsonFields2 extends AbstractProcessor {

    static final Map<String, String> NULL_REPRESENTATION_MAP = new HashMap<>();
    static final String EMPTY_STRING_OPTION = "empty string";
    static final String NULL_STRING_OPTION = "the string 'null'";

    static {
        NULL_REPRESENTATION_MAP.put(EMPTY_STRING_OPTION, "");
        NULL_REPRESENTATION_MAP.put(NULL_STRING_OPTION, "null");
    }

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_JSON = "json";
    public static final String RETURN_TYPE_SCALAR = "scalar";

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

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type").description("Indicates the desired return type of the JSON Path expressions.  Selecting 'auto-detect' will set the return type to 'json' "
                    + "for a Destination of 'flowfile-content', and 'scalar' for a Destination of 'flowfile-attribute'.")
            .required(true)
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_JSON, RETURN_TYPE_SCALAR)
            .defaultValue(RETURN_TYPE_AUTO)
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

    private final Map<String, String> attributeToJsonPathMap = new HashMap<>();
    private volatile String returnType = null;
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
        properties.add(RETURN_TYPE);
        properties.add(PATH_NOT_FOUND);
        properties.add(NULL_VALUE_DEFAULT_REPRESENTATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int jsonPathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    jsonPathCount++;
                }
            }

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
        return new PropertyDescriptor.Builder().name(propertyDescriptorName).expressionLanguageSupported(false).addValidator(
                new JsonPathValidator()).required(false).dynamic(true).build();
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
            attributeToJsonPathMap.put(entry.getKey().getName(), entry.getValue());
        }

        destination = processContext.getProperty(DESTINATION).getValue();
        returnType = processContext.getProperty(RETURN_TYPE).getValue();
        if (returnType.equals(RETURN_TYPE_AUTO)) {
            returnType = destination.equals(DESTINATION_CONTENT) ? RETURN_TYPE_JSON : RETURN_TYPE_SCALAR;
        }

    }


    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        /*DocumentContext documentContext = null;
        try {
            documentContext = validateAndEstablishJsonContext(processSession, flowFile);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile});
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }*/

        final String pathNotFound = processContext.getProperty(PATH_NOT_FOUND).getValue();

        JsonSurfer surfer = JsonSurfer.jackson();
        SurfingConfiguration.Builder configBuilder = surfer.configBuilder();

        final Map<String, List<Object>> jsonPathResults = new HashMap<>();
        final Map<String, String> attributeResults = new HashMap<>();

        for (final Map.Entry<String, String> attributeJsonPathEntry : attributeToJsonPathMap.entrySet()) {

            final String jsonPathAttrKey = attributeJsonPathEntry.getKey();
            final String jsonPathExp = attributeJsonPathEntry.getValue();

            jsonPathResults.put(jsonPathAttrKey, new LinkedList<>());

            try {
                configBuilder.bind(jsonPathExp, new JsonPathListener() {
                    @Override
                    public void onValue(Object value, ParsingContext context) throws Exception {
                        jsonPathResults.get(jsonPathAttrKey).add(value);
                    }
                });
            } catch (ParseCancellationException pce) {
                logger.error("ExtractJsonFields does not support the JSON Path expression {}.", new Object[]{jsonPathExp}, pce);
                processSession.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        try (InputStream inputStream = new BufferedInputStream(processSession.read(flowFile))) {
            configBuilder.buildAndSurf(new InputStreamReader(inputStream));
        } catch (IOException | JsonSurfingException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile}, e);
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        for (Map.Entry<String, List<Object>> jsonPathResult : jsonPathResults.entrySet()) {
            final String jsonPathAttrKey = jsonPathResult.getKey();
            List<Object> resultList = jsonPathResult.getValue();
            if (!resultList.isEmpty()) {
                if (returnType.equals(RETURN_TYPE_SCALAR) && (resultList.size() != 1 || !isJsonScalar(resultList.get(0)))) {
                    logger.error("Unable to return a scalar value for the expression {} for FlowFile {}. Evaluated value was {}. Transferring to {}.",
                            new Object[]{attributeToJsonPathMap.get(jsonPathResult.getKey()), flowFile.getId(), resultList.get(0).toString(), REL_FAILURE.getName()});
                    processSession.transfer(flowFile, REL_FAILURE);
                    return;
                }

            } else {
                if (pathNotFound.equals(PATH_NOT_FOUND_WARN)) {
                    logger.warn("FlowFile {} could not find path {} for attribute key {}.",
                            new Object[]{flowFile.getId(), attributeToJsonPathMap.get(jsonPathResult.getKey()), jsonPathAttrKey});
                }

                if (destination.equals(DESTINATION_ATTRIBUTE)) {
                    attributeResults.put(jsonPathAttrKey, StringUtils.EMPTY);
                    continue;
                } else {
                    processSession.transfer(flowFile, REL_NO_MATCH);
                    return;
                }
            }

            Object resultObject;
            if (resultList.size() == 1) {
                resultObject = resultList.get(0);
            } else {
                JSONArray a = new JSONArray();
                a.addAll(resultList);
                resultObject = a;
            }

            final String resultRepresentation = getResultRepresentation(resultObject, nullDefaultValue);
            switch (destination) {
                case DESTINATION_ATTRIBUTE:
                    attributeResults.put(jsonPathAttrKey, resultRepresentation);
                    break;
                case DESTINATION_CONTENT:
                    flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(resultRepresentation.getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    });
                    processSession.getProvenanceReporter().modifyContent(flowFile, "Replaced content with result of expression " + attributeToJsonPathMap.get(jsonPathResult.getKey()));
                    break;
            }
        }

        flowFile = processSession.putAllAttributes(flowFile, attributeResults);
        processSession.transfer(flowFile, REL_MATCH);
    }

    /**
     * Determines the context by which JsonSmartJsonProvider would treat the value. {@link Map} and {@link List} objects can be rendered as JSON elements, everything else is
     * treated as a scalar.
     *
     * @param obj item to be inspected if it is a scalar or a JSON element
     * @return false, if the object is a supported type; true otherwise
     */
    static boolean isJsonScalar(Object obj) {
        return !(obj instanceof JSONObject || obj instanceof JSONArray);
    }

    private static class JsonPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            String error = JsonPathExpressionValidator.isValidExpression(input) ? null : "specified expression was not valid: " + input;
            return new ValidationResult.Builder().subject(subject).valid(error == null).explanation(error).build();
        }
    }
}
