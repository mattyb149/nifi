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
package org.apache.nifi.registry.db;

import oracle.jdbc.datasource.OracleDataSource;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.jdbc.JdbcDatabaseDelegate;

import javax.annotation.PostConstruct;
import javax.script.ScriptException;
import javax.sql.DataSource;
import java.sql.SQLException;

public abstract class OracleDataSourceFactory extends TestDataSourceFactory {

    protected abstract OracleContainer oracleContainer();

    @Override
    protected DataSource createDataSource() {
        try (final OracleContainer container = oracleContainer()) {
            final OracleDataSource dataSource = new OracleConnectionPoolDataSource();
            dataSource.setURL(container.getJdbcUrl());
            dataSource.setUser(container.getUsername());
            dataSource.setPassword(container.getPassword());
            dataSource.setDatabaseName(container.getDatabaseName());
            return dataSource;
        } catch (SQLException sqle) {
            throw new RuntimeException("Error configuring Oracle Data Source", sqle);
        }
    }

    @PostConstruct
    public void initDatabase() throws SQLException, ScriptException {
        DatabaseDelegate databaseDelegate = new JdbcDatabaseDelegate(oracleContainer(), "");
        databaseDelegate.execute("DROP DATABASE test; CREATE DATABASE test;", "", 0, false, true);
    }
}
