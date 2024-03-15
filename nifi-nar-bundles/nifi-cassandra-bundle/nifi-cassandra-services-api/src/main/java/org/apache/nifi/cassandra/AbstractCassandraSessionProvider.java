package org.apache.nifi.cassandra;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextService;

public abstract class AbstractCassandraSessionProvider extends AbstractControllerService implements CassandraSessionProviderService {

        public static final int DEFAULT_CASSANDRA_PORT = 9042;

        // Common descriptors
        public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
                .name("Cassandra Contact Points")
                .description("Contact points are addresses of Cassandra nodes. The list of contact points should be "
                        + "comma-separated and in hostname:port format. Example node1:port,node2:port,...."
                        + " The default client port for Cassandra is 9042, but the port(s) must be explicitly specified.")
                .required(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
                .build();

        public static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
                .name("Keyspace")
                .description("The Cassandra Keyspace to connect to. If no keyspace is specified, the query will need to " +
                        "include the keyspace name before any table reference, in case of 'query' native processors or " +
                        "if the processor supports the 'Table' property, the keyspace name has to be provided with the " +
                        "table name in the form of <KEYSPACE>.<TABLE>")
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
                .name("SSL Context Service")
                .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                        + "connections.")
                .required(false)
                .identifiesControllerService(SSLContextService.class)
                .build();

        public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
                .name("Client Auth")
                .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                        + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                        + "has been defined and enabled.")
                .required(false)
                .allowableValues(ClientAuth.values())
                .defaultValue("REQUIRED")
                .build();

        public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
                .name("Username")
                .description("Username to access the Cassandra cluster")
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
                .name("Password")
                .description("Password to access the Cassandra cluster")
                .required(false)
                .sensitive(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        public final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
                .name("Consistency Level")
                .description("The strategy for how many replicas must respond before results are returned.")
                .required(true)
                .allowableValues(this.getConsistencyLevelValues())
                .defaultValue("ONE")
                .build();

        public static final PropertyDescriptor READ_TIMEOUT_MS = new PropertyDescriptor.Builder()
                .name("read-timeout-ms")
                .displayName("Read Timout (ms)")
                .description("Read timeout (in milliseconds). 0 means no timeout. If no value is set, the underlying default will be used.")
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
                .build();

        public static final PropertyDescriptor CONNECT_TIMEOUT_MS = new PropertyDescriptor.Builder()
                .name("connect-timeout-ms")
                .displayName("Connect Timeout (ms)")
                .description("Connection timeout (in milliseconds). 0 means no timeout. If no value is set, the underlying default will be used.")
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
                .build();

        protected abstract Object[] getConsistencyLevelValues();

}