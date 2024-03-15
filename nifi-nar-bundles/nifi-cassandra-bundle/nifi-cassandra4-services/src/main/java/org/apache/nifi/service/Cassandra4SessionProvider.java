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
package org.apache.nifi.service;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.SSLContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.session.Session;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.cassandra.AbstractCassandraSessionProvider;
import org.apache.nifi.cassandra.CassandraSession;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_COMPRESSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TRACE_CONSISTENCY;

@Tags({"cassandra", "dbcp", "database", "connection", "pooling"})
@CapabilityDescription("Provides connection session for Cassandra processors to work with Apache Cassandra.")
public class Cassandra4SessionProvider extends AbstractCassandraSessionProvider {

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Enable compression at transport-level requests and responses")
            .required(false)
            .allowableValues("NONE", "LZ4", "SNAPPY")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NONE")
            .build();
    private List<PropertyDescriptor> properties;
    private Session session;
    private CassandraSession cassandraSession;

    @Override
    public void init(final ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();

        props.add(CONTACT_POINTS);
        props.add(CLIENT_AUTH);
        props.add(CONSISTENCY_LEVEL);
        props.add(COMPRESSION_TYPE);
        props.add(KEYSPACE);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(PROP_SSL_CONTEXT_SERVICE);
        props.add(READ_TIMEOUT_MS);
        props.add(CONNECT_TIMEOUT_MS);

        properties = props;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        connectToCassandra(context);
    }

    @OnDisabled
    public void onDisabled() {
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
    }

    @Override
    public CassandraSession getCassandraSession() {
        if (cassandraSession != null) {
            return cassandraSession;
        } else {
            throw new ProcessException("Unable to get the Cassandra session.");
        }
    }

    private void connectToCassandra(ConfigurationContext context) {
        if (session == null) {
            ComponentLog log = getLogger();
            final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();
            final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
            final String compressionType = context.getProperty(COMPRESSION_TYPE).getValue();

            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            // Set up the client for secure (SSL/TLS communications) if configured to do so
            final SSLContextService sslService =
                    context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final SSLContext sslContext;

            if (sslService == null) {
                sslContext = null;
            } else {
                sslContext = sslService.createContext();
            }

            final String username, password;
            PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
            PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

            if (usernameProperty != null && passwordProperty != null) {
                username = usernameProperty.getValue();
                password = passwordProperty.getValue();
            } else {
                username = null;
                password = null;
            }

            final Integer readTimeoutMillis = context.getProperty(READ_TIMEOUT_MS).evaluateAttributeExpressions().asInteger();
            final Integer connectTimeoutMillis = context.getProperty(CONNECT_TIMEOUT_MS).evaluateAttributeExpressions().asInteger();

            // Connect to the cluster
            PropertyValue keyspaceProperty = context.getProperty(KEYSPACE).evaluateAttributeExpressions();
            session = createSession(contactPoints, sslContext, username, password, compressionType, readTimeoutMillis,
                    connectTimeoutMillis, consistencyLevel, keyspaceProperty.getValue());

            cassandraSession = new Cassandra4Session(session);
            log.info("Connected to Cassandra cluster: {}", getClusterName());
        }
    }

    private List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }

        final String[] contactPointStringList = contactPointList.split(",");
        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (String contactPointEntry : contactPointStringList) {
            String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }

        return contactPoints;
    }

    private Session createSession(final List<InetSocketAddress> contactPoints, final SSLContext sslContext,
                                  final String username, final String password, final String compressionType,
                                  final Integer readTimeoutMillis, final Integer connectTimeoutMillis,
                                  final String consistencyLevel, final String keyspaceName) {

        ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
                DriverConfigLoader.programmaticBuilder().withString(PROTOCOL_COMPRESSION, compressionType);
        if (connectTimeoutMillis != null) {
            driverConfigLoaderBuilder.withDuration(CONNECTION_CONNECT_TIMEOUT, Duration.ofMillis(connectTimeoutMillis));
        }
        if (readTimeoutMillis != null) {
            driverConfigLoaderBuilder.withDuration(REQUEST_TIMEOUT, Duration.ofMillis(readTimeoutMillis));
        }
        if (consistencyLevel != null) {
            driverConfigLoaderBuilder.withString(REQUEST_CONSISTENCY, consistencyLevel);
            driverConfigLoaderBuilder.withString(REQUEST_SERIAL_CONSISTENCY, consistencyLevel);
            driverConfigLoaderBuilder.withString(REQUEST_TRACE_CONSISTENCY, consistencyLevel);
        }
        CqlSessionBuilder builder = CqlSession.builder()
                .withConfigLoader(driverConfigLoaderBuilder.build())
                .addContactPoints(contactPoints)
                .withKeyspace(keyspaceName);
        if (sslContext != null) {
            builder = builder.withSslContext(sslContext);
        }

        if (username != null && password != null) {
            builder = builder.withAuthCredentials(username, password);
        }

        return builder.build();
    }

    @Override
    public Optional<String> getClusterName() {
        if (session != null) {
            return session.getMetadata().getClusterName();
        }
        return Optional.empty();
    }

    @Override
    protected Object[] getConsistencyLevelValues() {
        return DefaultConsistencyLevel.values();
    }

}
