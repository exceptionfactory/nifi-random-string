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
package local.nifi;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrependRandomStringTest {

    private static final String INPUT_CONTENT = "Input Content";

    private static final int RANDOM_STRING_LENGTH_CONFIGURED = 10;

    private static final int EXPECTED_OUTPUT_LENGTH = INPUT_CONTENT.length() + RANDOM_STRING_LENGTH_CONFIGURED;

    private TestRunner testRunner;

    @BeforeEach
    void setTestRunner() {
        testRunner = TestRunners.newTestRunner(PrependRandomString.class);
    }

    @Test
    void testSuccess() {
        testRunner.setProperty(PrependRandomString.RANDOM_STRING_LENGTH, Integer.toString(RANDOM_STRING_LENGTH_CONFIGURED));

        testRunner.enqueue(INPUT_CONTENT);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PrependRandomString.SUCCESS);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PrependRandomString.SUCCESS).getFirst();
        final String flowFileContent = flowFile.getContent();

        assertTrue(flowFileContent.endsWith(INPUT_CONTENT));
        assertEquals(EXPECTED_OUTPUT_LENGTH, flowFileContent.length());
    }
}
