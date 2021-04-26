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
package org.apache.nifi.dbcp;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of Database Connection Pooling Service. HikariCP is used for connection pooling functionality.
 */
@RequiresInstanceClassLoading
@Tags({"dbcp", "hikari", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service based on HikariCP. Connections can be asked from pool and returned after usage.")
@DynamicProperty(name = "JDBC property name", value = "JDBC property value", expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Specifies a property name and value to be set on the JDBC connection(s). "
                + "If Expression Language is used, evaluation will be performed upon the controller service being enabled. "
                + "Note that no flow file input (attributes, e.g.) is available for use in Expression Language constructs for these properties.")
public class HikariCPConnectionPool extends AbstractControllerService implements DBCPService {
    /** Property Name Prefix for Sensitive Dynamic Properties */
    protected static final String SENSITIVE_PROPERTY_PREFIX = "SENSITIVE.";

    private static final String DEFAULT_TOTAL_CONNECTIONS = "10";
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("hikaricp-connection-url")
            .displayName("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by your DBMS.")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("hikaricp-driver-classname")
            .displayName("Database Driver Class Name")
            .description("The fully-qualified class name of the JDBC driver. Example: com.mysql.jdbc.Driver")
            .defaultValue(null)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("hikaricp-driver-locations")
            .displayName("Database Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .defaultValue(null)
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("hikaricp-username")
            .displayName("Database User")
            .description("Database user name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("hikaricp-password")
            .displayName("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("hikaricp-max-wait-time")
            .displayName("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or 0 <time units> to wait indefinitely. ")
            .defaultValue("30 sec")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("hikaricp-max-total-conns")
            .displayName("Max Total Connections")
            .description("This property controls the maximum size that the pool is allowed to reach, including both idle and in-use connections. Basically this value will determine the "
                    + "maximum number of actual connections to the database backend. A reasonable value for this is best determined by your execution environment. When the pool reaches "
                    + "this size, and no idle connections are available, the service will block for up to connectionTimeout milliseconds before timing out.")
            .defaultValue(DEFAULT_TOTAL_CONNECTIONS)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("hikaricp-validation-query")
            .displayName("Validation Query")
            .description("Validation Query used to validate connections before returning them. "
                    + "When connection is invalid, it gets dropped and new valid connection will be returned. "
                    + "NOTE: Using validation might have some performance penalty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ALIVE_BYPASS_WINDOW = new PropertyDescriptor.Builder()
            .name("hikaricp-alive-bypass-window")
            .displayName("Alive Bypass Window")
            .description("Specifies that if a connection has not been used within the time window, HikariCP will test it. It is recommended that this window be less than 5 seconds. "
                    + "If a network partition event occurs in the 5 seconds between when a connection is returned and when it is borrowed again, the processor may hang due to "
                    + "unacknowledged TCP packets. Since this property is set as a system-level property, it is recommended that every instance of HikariCPConnectionPool have the same "
                    + "value for this property.")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .name("hikaricp-min-idle-conns")
            .displayName("Minimum Idle Connections")
            .description("This property controls the minimum number of idle connections that HikariCP tries to maintain in the pool. If the idle connections dip below this value and total "
                    + "connections in the pool are less than 'Max Total Connections', HikariCP will make a best effort to add additional connections quickly and efficiently. It is recommended "
                    + "that this property to be set equal to 'Max Total Connections'.")
            .defaultValue(DEFAULT_TOTAL_CONNECTIONS)
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .name("hikaricp-max-conn-lifetime")
            .displayName("Max Connection Lifetime")
            .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("kerberos-principal")
            .displayName("Kerberos Principal")
            .description("The principal to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor KERBEROS_PASSWORD = new PropertyDescriptor.Builder()
            .name("kerberos-password")
            .displayName("Kerberos Password")
            .description("The password to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_LOCATION);
        props.add(KERBEROS_CREDENTIALS_SERVICE);
        props.add(KERBEROS_PRINCIPAL);
        props.add(KERBEROS_PASSWORD);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(ALIVE_BYPASS_WINDOW);
        props.add(MIN_IDLE);
        props.add(MAX_CONN_LIFETIME);

        properties = Collections.unmodifiableList(props);
    }

    private volatile HikariDataSource dataSource;
    private volatile KerberosUser kerberosUser;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        if (propertyDescriptorName.startsWith(SENSITIVE_PROPERTY_PREFIX)) {
            builder.sensitive(true).expressionLanguageSupported(ExpressionLanguageScope.NONE);
        } else {
            builder.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY);
        }

        return builder.build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean kerberosPrincipalProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue());
        final boolean kerberosPasswordProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PASSWORD).getValue());

        if (kerberosPrincipalProvided && !kerberosPasswordProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PASSWORD.getDisplayName())
                    .valid(false)
                    .explanation("a password must be provided for the given principal")
                    .build());
        }

        if (kerberosPasswordProvided && !kerberosPrincipalProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PRINCIPAL.getDisplayName())
                    .valid(false)
                    .explanation("a principal must be provided for the given password")
                    .build());
        }

        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (kerberosCredentialsService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_CREDENTIALS_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos principal/password and kerberos credential service cannot be configured at the same time")
                    .build());
        }

        return results;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link HikariDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final String dburl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Long aliveBypassWindow = extractMillisWithInfinite(context.getProperty(ALIVE_BYPASS_WINDOW).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        final String kerberosPrincipal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String kerberosPassword = context.getProperty(KERBEROS_PASSWORD).getValue();

        if (kerberosCredentialsService != null) {
            kerberosUser = new KerberosKeytabUser(kerberosCredentialsService.getPrincipal(), kerberosCredentialsService.getKeytab());
        } else if (!StringUtils.isBlank(kerberosPrincipal) && !StringUtils.isBlank(kerberosPassword)) {
            kerberosUser = new KerberosPasswordUser(kerberosPrincipal, kerberosPassword);
        }

        if (kerberosUser != null) {
            try {
                kerberosUser.login();
            } catch (LoginException e) {
                throw new InitializationException("Unable to authenticate Kerberos principal", e);
            }
        }

        dataSource = new HikariDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setConnectionTimeout(maxWaitMillis);
        dataSource.setMaximumPoolSize(maxTotal);
        dataSource.setMinimumIdle(minIdle);
        dataSource.setMaxLifetime(maxConnLifetimeMillis);

        if (validationQuery != null && !validationQuery.isEmpty()) {
            dataSource.setConnectionTestQuery(validationQuery);
        }

        dataSource.setJdbcUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        // Alive Bypass Window is set via an undocumented system property in HikariCP
        System.setProperty("com.zaxxer.hikari.aliveBypassWindowMs", String.valueOf(aliveBypassWindow));

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        Properties properties = dataSource.getDataSourceProperties();
        dynamicProperties.forEach((descriptor) -> {
            final PropertyValue propertyValue = context.getProperty(descriptor);
            if (descriptor.isSensitive()) {
                final String propertyName = StringUtils.substringAfter(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX);
                properties.setProperty(propertyName, propertyValue.getValue());
            } else {
                properties.setProperty(descriptor.getName(), propertyValue.evaluateAttributeExpressions().getValue());
            }
        });
        dataSource.setDataSourceProperties(properties);

    }

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     * <p>
     * If a @{@link LoginException} occurs while attempting to log out the @{@link org.apache.nifi.security.krb.KerberosUser},
     * an attempt will still be made to shut down the pool and close open connections.
     *
     * @throws SQLException   if there is an error while closing open connections
     * @throws LoginException if there is an error during the principal log out, and will only be thrown if there was
     *                        no exception while closing open connections
     */
    @OnDisabled
    public void shutdown() throws SQLException, LoginException {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            kerberosUser = null;
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            } finally {
                dataSource = null;
            }
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            final Connection con;
            if (kerberosUser != null) {
                KerberosAction<Connection> kerberosAction = new KerberosAction<>(kerberosUser, () -> dataSource.getConnection(), getLogger());
                con = kerberosAction.execute();
            } else {
                con = dataSource.getConnection();
            }
            return con;
        } catch (final SQLException e) {
            // If using Kerberos,  attempt to re-login
            if (kerberosUser != null) {
                try {
                    getLogger().info("Error getting connection, performing Kerberos re-login");
                    kerberosUser.login();
                } catch (LoginException le) {
                    throw new ProcessException("Unable to authenticate Kerberos principal", le);
                }
            }
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return "DBCPConnectionPool[id=" + getIdentifier() + "]";
    }

    HikariDataSource getDataSource() {
        return dataSource;
    }
}
