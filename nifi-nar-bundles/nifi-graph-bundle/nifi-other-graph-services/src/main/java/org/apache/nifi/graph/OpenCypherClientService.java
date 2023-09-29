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

package org.apache.nifi.graph;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.graph.exception.GraphClientMethodNotSupported;
import org.apache.nifi.graph.exception.GraphQueryException;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.opencypher.gremlin.neo4j.driver.GremlinDatabase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CapabilityDescription("A client service that uses the OpenCypher implementation of the Cypher query language to connect to " +
        "databases other than Neo4J that are on the supported list of OpenCypher-compatible products. For more information, see: " +
        "http://www.opencypher.org/")
@Tags({ "cypher", "opencypher", "graph", "database", "janus" })
public class OpenCypherClientService extends AbstractTinkerpopClientService implements GraphClientService {
    private volatile Driver gremlinDriver;

    protected volatile QueryFromNodesBuilder cypherQueryFromNodesBuilder = new CypherQueryFromNodesBuilder();

    private String databaseName;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        Cluster cluster = buildCluster(context);

        gremlinDriver = GremlinDatabase.driver(cluster);
    }

    @OnDisabled
    public void onDisabled() {
        gremlinDriver.close();
    }

    public static final String NOT_SUPPORTED = "NOT_SUPPORTED";

    private Map<String, Object> handleInternalNode(Map<String, Object> recordMap) {
        if (recordMap.size() == 1) {
            String key = recordMap.keySet().iterator().next();
            Object value = recordMap.get(key);
            if (value instanceof InternalNode) {
                return ((InternalNode)value).asMap();
            }
        }

        return recordMap;
    }

    @Override
    public Map<String, String> executeQuery(GraphQuery graphQuery, Map<String, Object> parameters, GraphQueryResultCallback handler) throws GraphQueryException {
        try (Session session = gremlinDriver.session()) {
            StatementResult result = session.run(graphQuery.getQuery(), parameters);
            long count = 0;
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> asMap = handleInternalNode(record.asMap());
                handler.process(asMap, result.hasNext());
                count++;
            }

            Map<String,String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_CREATED, NOT_SUPPORTED);
            resultAttributes.put(LABELS_ADDED, NOT_SUPPORTED);
            resultAttributes.put(NODES_DELETED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_DELETED, NOT_SUPPORTED);
            resultAttributes.put(PROPERTIES_SET, NOT_SUPPORTED);
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;
        } catch (Exception ex) {
            throw new GraphQueryException(ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return transitUrl;
    }

    @Override
    public List<GraphQuery> convertActionsToQueries(final List<Map<String, Object>> nodeList) {
        return Collections.emptyList();
    }

    @Override
    public List<GraphQuery> buildFlowGraphQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters) {
        // Build queries from event list
        return cypherQueryFromNodesBuilder.getFlowGraphQueries(eventList);
    }

    @Override
    public List<GraphQuery> buildProvenanceQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters, final boolean includeFlowGraph) {
        // Build queries from event list
        return cypherQueryFromNodesBuilder.getProvenanceQueries(eventList, includeFlowGraph);
    }

    @Override
    public List<GraphQuery> generateCreateDatabaseQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateCreateDatabaseQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateCreateIndexQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateInitialVertexTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return cypherQueryFromNodesBuilder.generateInitialEdgeTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }
}
