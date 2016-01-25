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
package org.apache.nifi.processors.script;

import org.apache.nifi.util.MockFlowFile;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class TestExecuteGroovy extends BaseScriptTest {

    private final String TEST_CSV_DATA = "gender,title,first,last\n"
            + "female,miss,marlene,shaw\n"
            + "male,mr,todd,graham";

    /**
     * Tests a script file that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_FILE, "target/test/resources/groovy/test_onTrigger.groovy");
        runner.setProperty(ExecuteScript.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script file that creates and transfers a new flow file.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testCreateNewFlowFileWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_FILE, "target/test/resources/groovy/test_onTrigger_newFlowFile.groovy");
        runner.setProperty(ExecuteScript.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        // The script removes the original file and transfers only the new one
        assertEquals(1, runner.getRemovedCount());
        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("filename", "split_cols.txt");
    }

    /**
     * Tests a script file that changes the content of the incoming flowfile.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testChangeFlowFileWithScriptFile() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_FILE, "target/test/resources/groovy/test_onTrigger_changeContent.groovy");
        runner.setProperty(ExecuteScript.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        MockFlowFile resultFile = result.get(0);
        resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Marlene Shaw\nTodd Graham\n");
    }


    /**
     * Tests a script that has provides the body of an onTrigger() function.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBody() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY,
                "flowFile = session.putAttribute(flowFile, \"from-content\", \"test content\")\n"
                        + "session.transfer(flowFile, REL_SUCCESS)");
        runner.setProperty(ExecuteScript.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that has provides the body of an onTrigger() function, where the ExecuteScript processor does
     * not specify a modules path
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttributeWithScriptBodyNoModules() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY,
                "flowFile = session.putAttribute(flowFile, \"from-content\", \"test content\")\n"
                        + "session.transfer(flowFile, REL_SUCCESS)");
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that does not transfer or remove the original flow file, thereby causing an error during commit.
     *
     * @throws Exception Any error encountered while testing. Expecting
     */
    @Test(expected = AssertionError.class)
    public void testScriptNoTransfer() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY,
                "flowFile = session.putAttribute(flowFile, \"from-content\", \"test content\")\n");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

    }

    /**
     * Tests a script that uses a dynamic property to set a FlowFile attribute.
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileCustomAttribute() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY,
                "flowFile = session.putAttribute(flowFile, \"from-content\", \"${testprop}\")\n"
                        + "session.transfer(flowFile, REL_SUCCESS)");
        runner.setProperty("testprop", "test content");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_SUCCESS, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_SUCCESS);
        result.get(0).assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that throws an Exception within. The expected result is that the FlowFile will be routed to
     * failure
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testScriptException() throws Exception {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ExecuteScript.SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ExecuteScript.SCRIPT_BODY, "throw new Exception()");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteScript.REL_FAILURE, 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(ExecuteScript.REL_FAILURE);
        assertFalse(result.isEmpty());
    }
}
