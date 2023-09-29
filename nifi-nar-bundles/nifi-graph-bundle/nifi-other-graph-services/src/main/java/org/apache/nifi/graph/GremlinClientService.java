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
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


@CapabilityDescription("A client service that connects to a graph database that can accept queries in the Tinkerpop Gremlin DSL.")
@Tags({"graph", "database", "gremlin", "tinkerpop",})
public class GremlinClientService extends AbstractTinkerpopClientService implements TinkerPopClientService {
    private Cluster cluster;
    protected Client client;
    public static final String NOT_SUPPORTED = "NOT_SUPPORTED";
    private ConfigurationContext context;

    private String databaseName; // TODO add property to specify the database name?

    protected final QueryFromNodesBuilder gremlinQueryFromNodesBuilder = new GremlinQueryFromNodesBuilder();

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
        cluster = buildCluster(context);
        client = cluster.connect();
    }

    @OnDisabled
    public void onDisabled() {
        client.close();
        cluster.close();
        client = null;
        cluster = null;
    }

    public Map<String, String> doQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) throws GraphQueryException {
        try {
            Iterator<Result> iterator = client.submit(query, parameters).iterator();
            long count = 0;
            while (iterator.hasNext()) {
                Result result = iterator.next();
                Object obj = result.getObject();
                if (obj instanceof Map) {
                    handler.process((Map) obj, iterator.hasNext());
                } else {
                    handler.process(new HashMap<String, Object>() {{
                        put("result", obj);
                    }}, iterator.hasNext());
                }
                count++;
            }

            Map<String, String> resultAttributes = new HashMap<>();
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
    public Map<String, String> executeQuery(GraphQuery graphQuery, Map<String, Object> parameters, GraphQueryResultCallback handler) throws GraphQueryException {
        final String query = graphQuery.getQuery();
        try {
            return doQuery(query, parameters, handler);
        } catch (Exception ex) {
            cluster.close();
            client.close();
            cluster = buildCluster(context);
            client = cluster.connect();
            return doQuery(query, parameters, handler);
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
        return gremlinQueryFromNodesBuilder.getFlowGraphQueries(eventList);
    }

    @Override
    public List<GraphQuery> buildProvenanceQueriesFromNodes(List<Map<String, Object>> eventList, Map<String, Object> parameters, final boolean includeFlowGraph) {
        // Build queries from event list
        return gremlinQueryFromNodesBuilder.getProvenanceQueries(eventList, includeFlowGraph);
    }

    @Override
    public List<GraphQuery> generateCreateDatabaseQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateCreateDatabaseQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateCreateIndexQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateCreateIndexQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialVertexTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateInitialVertexTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public List<GraphQuery> generateInitialEdgeTypeQueries(final String databaseName, final boolean isCompositeDatabase) throws GraphClientMethodNotSupported {
        return gremlinQueryFromNodesBuilder.generateInitialEdgeTypeQueries(databaseName, isCompositeDatabase);
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

}
