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
package org.apache.nifi.lookup.db;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.util.db.JdbcCommon;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleDatabaseLookupService extends AbstractDatabaseLookupService implements StringLookupService {

    private volatile Cache<Tuple<String, Object>, String> cache;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile String lookupValueColumn;

    static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
            new PropertyDescriptor.Builder()
                    .name("lookup-value-column")
                    .displayName("Lookup Value Column")
                    .description("The column whose value will be returned when the Lookup value is matched")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DBCP_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.add(LOOKUP_VALUE_COLUMN);
        properties.add(CACHE_SIZE);
        properties.add(CLEAR_CACHE_ON_ENABLED);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        this.lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).evaluateAttributeExpressions().getValue();
        this.lookupValueColumn = context.getProperty(LOOKUP_VALUE_COLUMN).evaluateAttributeExpressions().getValue();
        int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            this.cache = Caffeine.newBuilder()
                    .maximumSize(cacheSize)
                    .build();
        }

        options = JdbcCommon.AvroConversionOptions.builder()
                .recordName("NiFi_DB_Record_Lookup")
                // Ignore duplicates
                .maxRows(1)
                // Keep column names as field names
                .convertNames(false)
                // Get the value as a string, so logical types are irrelevant
                .useLogicalTypes(false)
                .build();
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        return lookup(coordinates, null);
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        final Object key = coordinates.get(KEY);
        if (StringUtils.isBlank(key.toString())) {
            return Optional.empty();
        }

        final String tableName = getProperty(TABLE_NAME).evaluateAttributeExpressions(context).getValue();

        Tuple<String, Object> cacheLookupKey = new Tuple<>(tableName, key);

        // Not using the function param of cache.get so we can catch and handle the checked exceptions
        String foundRecord = cache.get(cacheLookupKey, k -> null);

        if (foundRecord == null) {
            final String selectQuery = "SELECT " + lookupValueColumn + " FROM " + tableName + " WHERE " + lookupKeyColumn + " = ?";
            try (final Connection con = dbcpService.getConnection(context);
                 final PreparedStatement st = con.prepareStatement(selectQuery)) {

                st.setObject(1, key);
                ResultSet resultSet = st.executeQuery();

                if (!resultSet.next()) {
                    return Optional.empty();
                }

                Object o = resultSet.getObject(lookupValueColumn);
                if (o == null) {
                    return Optional.empty();
                }
                foundRecord = o.toString();

                // Populate the cache if the record is present
                if (foundRecord != null) {
                    lock.writeLock().lock();
                    cache.put(cacheLookupKey, foundRecord);
                    lock.writeLock().unlock();
                }

            } catch (SQLException se) {
                throw new LookupFailureException("Error executing SQL statement: " + selectQuery + "for value " + key.toString(), se);
            }
        }

        return Optional.ofNullable(foundRecord);
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
