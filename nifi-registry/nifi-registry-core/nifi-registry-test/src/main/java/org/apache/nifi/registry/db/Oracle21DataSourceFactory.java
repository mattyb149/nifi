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

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.testcontainers.containers.OracleContainer;

@Configuration
@Profile({"oracle", "oracle-21"})
public class Oracle21DataSourceFactory extends OracleDataSourceFactory {

    private static final OracleContainer ORACLE_CONTAINER = new OracleContainer("gvenzl/oracle-xe:21.3.0");

    static {
        ORACLE_CONTAINER.start();
    }

    @Override
    protected OracleContainer oracleContainer() {
        return ORACLE_CONTAINER;
    }
}
