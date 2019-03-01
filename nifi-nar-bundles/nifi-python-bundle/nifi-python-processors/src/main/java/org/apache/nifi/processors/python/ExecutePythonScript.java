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
package org.apache.nifi.processors.python;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;
import py4j.GatewayServer;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"python", "script"})
@CapabilityDescription("Provide a description")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ExecutePythonScript extends AbstractProcessor {

    public static final int DEFAULT_PORT = 25333;

    /**
     * A property descriptor for specifying the location of a script file
     */
    static final PropertyDescriptor PYTHON_LOCATION = new PropertyDescriptor.Builder()
            .name("execute-python-interpreter location")
            .displayName("Path to Python interpreter")
            .required(true)
            .description("Path to the location of the Python interpreter to use. This must point at the executable program itself, not the directory in which it resides.")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .description("The directory to use as the current working directory when executing the command")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
            .required(false)
            .build();

    /**
     * A property descriptor for specifying the location of a script file
     */
    static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .description("Path to script file to execute. Only one of Script File or Script Body may be used")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * A property descriptor for specifying the body of a script
     */
    static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("Script Body")
            .required(false)
            .description("Body of script to execute. Only one of Script File or Script Body may be used")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor GATEWAY_SERVER_HOSTNAME = new PropertyDescriptor.Builder()
            .name("execute-python-gateway-hostname")
            .displayName("Gateway Server Hostname")
            .description("The hostname to use for creating the Python Gateway Server.")
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor GATEWAY_SERVER_PORT = new PropertyDescriptor.Builder()
            .name("execute-python-gateway-port")
            .displayName("Gateway Server Port")
            .description("The port number to use for Python clients connecting to the Python Gateway Server.")
            .defaultValue(Integer.toString(DEFAULT_PORT))
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("execute-python-connect-timeout")
            .displayName("Connect Timeout")
            .description("The amount of time to wait for a connection to the Python Gateway Server to finish.")
            .required(true)
            .defaultValue("5 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("execute-python-read-timeout")
            .displayName("Read Timeout")
            .description("The amount of time to wait for a read request of the Python Gateway Server to finish.")
            .required(true)
            .defaultValue("5 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final AtomicReference<GatewayServer> gatewayServer = new AtomicReference<>(null);

    private final EntryPoint entryPoint = new EntryPoint();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PYTHON_LOCATION);
        descriptors.add(SCRIPT_FILE);
        descriptors.add(SCRIPT_BODY);
        descriptors.add(GATEWAY_SERVER_HOSTNAME);
        descriptors.add(GATEWAY_SERVER_PORT);
        descriptors.add(CONNECT_TIMEOUT);
        descriptors.add(READ_TIMEOUT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /**
     * Custom validation for ensuring exactly one of Script File or Script Body is populated
     *
     * @param validationContext provides a mechanism for obtaining externally
     *                          managed values, such as property values and supplies convenience methods
     *                          for operating on those values
     * @return A collection of validation results
     */
    public Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Verify that exactly one of "script file" or "script body" is set
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.get(SCRIPT_FILE)) == StringUtils.isEmpty(propertyMap.get(SCRIPT_BODY))) {
            results.add(new ValidationResult.Builder().subject("Script Body or Script File").valid(false).explanation(
                    "exactly one of Script File or Script Body must be set").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (gatewayServer.get() == null) {
            synchronized (gatewayServer) {
                if (gatewayServer.get() == null) {
                    String hostname = "";
                    try {
                        hostname = context.getProperty(GATEWAY_SERVER_HOSTNAME).evaluateAttributeExpressions().getValue();
                        InetAddress inetAddress = InetAddress.getByName(hostname);
                        int port = context.getProperty(GATEWAY_SERVER_PORT).evaluateAttributeExpressions().asInteger();
                        long connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
                        long readTimeout = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
                        GatewayServer server = new GatewayServer(entryPoint, port, 0, inetAddress, inetAddress, (int) connectTimeout, (int) readTimeout, null);
                        server.start(true);
                        gatewayServer.set(server);
                    } catch (UnknownHostException uhe) {
                        throw new ProcessException("Could not connect to hostname: " + hostname, uhe);
                    }
                }
            }
        }

        // Add the context/session pair to the queue
        entryPoint.add(context, session);

        // Now that it is available to the EntryPoint, start a Python process to retrieve it
        String pythonCommand = context.getProperty(PYTHON_LOCATION).evaluateAttributeExpressions().getValue();
        final String workingDir = context.getProperty(WORKING_DIR).evaluateAttributeExpressions().getValue();

        final ComponentLog logger = getLogger();
        final ProcessBuilder builder = new ProcessBuilder();

        logger.debug("Executing and waiting for command {}", new Object[]{pythonCommand});
        File dir = null;
        if (!StringUtils.isBlank(workingDir)) {
            dir = new File(workingDir);
            if (!dir.exists() && !dir.mkdirs()) {
                logger.warn("Failed to create working directory {}, using current working directory {}", new Object[]{workingDir, System.getProperty("user.dir")});
            }
        }
        final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }
        builder.environment().putAll(environment);
        builder.command();
        builder.directory(dir);
        builder.redirectInput(ProcessBuilder.Redirect.PIPE);
        builder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        final File errorOut;
        try {
            errorOut = File.createTempFile("out", null);
            builder.redirectError(errorOut);
        } catch (IOException e) {
            logger.error("Could not create temporary file for error logging", e);
            throw new ProcessException(e);
        }

        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            logger.error("Could not create external process to run command", e);
            throw new ProcessException(e);
        }

        try {
            process.waitFor();
        } catch (InterruptedException ie) {
            // TODO Process didn't finish
        }
    }

    @OnStopped
    public void onStopped() {
        if (gatewayServer.get() != null) {
            gatewayServer.get().shutdown();
            gatewayServer.set(null);
        }
    }


    private static class EntryPoint {
        LinkedBlockingQueue<Tuple<ProcessContext, ProcessSession>> sessionContextQ;

        public void add(ProcessContext context, ProcessSession session) {
            sessionContextQ.add(new Tuple<>(context, session));
        }

        public Tuple<ProcessContext, ProcessSession> get() {
            try {
                return sessionContextQ.take();
            } catch (InterruptedException ie) {
                return null;
            }
        }
    }
}
