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
package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestFetchElasticsearch {

    private InputStream twitterExample;
    private TestRunner runner;

    @Before
    public void setUp() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        twitterExample = classloader
                .getResourceAsStream("TweetExample.json");

    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testFetchElasticsearchOnTrigger() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(true)); // all docs are found
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "tweet");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.DOC_ID, "${tweet_id}");
        runner.assertValid();

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652140");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("tweet_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerWithFailures() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(false)); // simulate doc not found
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "tweet");
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setValidateExpressionUsage(true);
        runner.setProperty(FetchElasticsearch.DOC_ID, "${tweet_id}");

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652140");
        }});
        runner.run(1, true, true);

        // This test generates a "document not found", which gets routed to retry
        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_RETRY, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_RETRY).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("tweet_id", "28039652140");
    }

    @Test
    public void testFetchElasticsearchOnTriggerNode() throws IOException {
        runner = TestRunners.newTestRunner(new FetchElasticsearchTestProcessor(true)); // no failures
        runner.setValidateExpressionUsage(true);
        runner.setProperty(AbstractElasticsearchProcessor.CLIENT_TYPE, "node");
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "tweet");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.assertNotValid();
        runner.setProperty(FetchElasticsearch.DOC_ID, "${tweet_id}");
        runner.assertValid();

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652141");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("tweet_id", "28039652141");
    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class FetchElasticsearchTestProcessor extends FetchElasticsearch {
        boolean documentExists = true;

        public FetchElasticsearchTestProcessor(boolean documentExists) {
            this.documentExists = documentExists;
        }

        @Override
        @OnScheduled
        public void createClient(ProcessContext context) throws IOException {
            Client mockClient = mock(Client.class);

            GetRequestBuilder getRequestBuilder = spy(new GetRequestBuilder(mockClient, GetAction.INSTANCE));
            doReturn(new MockGetRequestBuilderExecutor(documentExists)).when(getRequestBuilder).execute();
            when(mockClient.prepareGet(anyString(), anyString(), anyString())).thenReturn(getRequestBuilder);

            esClient.set(mockClient);
        }

        private static class MockGetRequestBuilderExecutor
                extends AdapterActionFuture<GetResponse, ActionListener<GetResponse>>
                implements ListenableActionFuture<GetResponse> {

            boolean documentExists = true;

            public MockGetRequestBuilderExecutor(boolean documentExists) {
                this.documentExists = documentExists;
            }


            @Override
            protected GetResponse convert(ActionListener<GetResponse> bulkResponseActionListener) {
                return null;
            }

            @Override
            public void addListener(ActionListener<GetResponse> actionListener) {

            }

            @Override
            public GetResponse get() throws InterruptedException, ExecutionException {
                GetResponse response = mock(GetResponse.class);
                when(response.isExists()).thenReturn(documentExists);
                when(response.getSourceAsBytes()).thenReturn("Success".getBytes());
                return response;
            }

            @Override
            public GetResponse actionGet() {
                try {
                    return get();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
                return null;
            }
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Integration test section below
    //
    // The tests below are meant to run on real ES instances, and are thus @Ignored during normal test execution.
    // However if you wish to execute them as part of a test phase, comment out the @Ignored line for each
    // desired test.
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Tests basic ES functionality against a local or test ES cluster
     *
     * @throws IOException
     */
    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testFetchElasticsearchBasic() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());
        runner.setValidateExpressionUsage(true);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchProcessor.CLIENT_TYPE, "transport");
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");

        runner.setProperty(FetchElasticsearch.INDEX, "tweet");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${tweet_id}");
        runner.assertValid();

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652140");
        }});


        runner.enqueue(twitterExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);

    }

    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testFetchElasticsearchBasicNode() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());
        runner.setValidateExpressionUsage(true);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchProcessor.CLIENT_TYPE, "node");
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.PATH_HOME, "/usr/local/opt/elasticsearch");
        runner.setProperty(FetchElasticsearch.INDEX, "tweet");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${tweet_id}");
        runner.assertValid();

        runner.enqueue(twitterExample, new HashMap<String, String>() {{
            put("tweet_id", "28039652141");
        }});

        runner.enqueue(twitterExample);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);

    }

    @Test
    @Ignore("Comment this out if you want to run against local or test ES")
    public void testFetchElasticsearchBatch() throws IOException {
        System.out.println("Starting test " + new Object() {
        }.getClass().getEnclosingMethod().getName());
        final TestRunner runner = TestRunners.newTestRunner(new FetchElasticsearch());
        runner.setValidateExpressionUsage(true);

        //Local Cluster - Mac pulled from brew
        runner.setProperty(AbstractElasticsearchProcessor.CLIENT_TYPE, "transport");
        runner.setProperty(AbstractElasticsearchProcessor.CLUSTER_NAME, "elasticsearch_brew");
        runner.setProperty(AbstractElasticsearchProcessor.HOSTS, "127.0.0.1:9300");
        runner.setProperty(AbstractElasticsearchProcessor.PING_TIMEOUT, "5s");
        runner.setProperty(AbstractElasticsearchProcessor.SAMPLER_INTERVAL, "5s");
        runner.setProperty(FetchElasticsearch.INDEX, "tweet");

        runner.setProperty(FetchElasticsearch.TYPE, "status");
        runner.setProperty(FetchElasticsearch.DOC_ID, "${tweet_id}");
        runner.assertValid();


        String message = convertStreamToString(twitterExample);
        for (int i = 0; i < 100; i++) {

            long newId = 28039652140L + i;
            final String newStrId = Long.toString(newId);
            runner.enqueue(message.getBytes(), new HashMap<String, String>() {{
                put("tweet_id", newStrId);
            }});

        }

        runner.run();

        runner.assertAllFlowFilesTransferred(FetchElasticsearch.REL_SUCCESS, 100);
        final MockFlowFile out = runner.getFlowFilesForRelationship(FetchElasticsearch.REL_SUCCESS).get(0);

    }

    /**
     * Convert an input stream to a stream
     *
     * @param is input the input stream
     * @return return the converted input stream as a string
     */
    static String convertStreamToString(InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
