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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import org.apache.nifi.cassandra.CassandraBoundStatement;
import org.apache.nifi.cassandra.CassandraPreparedStatement;
import org.apache.nifi.cassandra.CassandraResultSetFuture;
import org.apache.nifi.cassandra.CassandraSession;

import java.util.Optional;


public class Cassandra4Session implements CassandraSession {

    protected CqlSession session;

    public Cassandra4Session(CqlSession session) {
        this.session = session;
    }

    @Override
    public CassandraPreparedStatement prepare(String s) {
        return () -> new Cassandra4PreparedStatement(session.prepare(s));
    }

    @Override
    public CassandraResultSetFuture executeAsync(CassandraBoundStatement boundStatement) {
        // TODO
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public Optional<String> getClusterName() {
        return session.getMetadata().getClusterName();
    }
}