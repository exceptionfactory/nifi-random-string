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

import org.apache.commons.text.RandomStringGenerator;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

@Tags({"prepend", "random"})
@CapabilityDescription("Prepend a random string to provided FlowFiles")
public class PrependRandomString extends AbstractProcessor {

    static final PropertyDescriptor RANDOM_STRING_LENGTH = new PropertyDescriptor
            .Builder()
            .name("Random String Length")
            .description("Length of the random string")
            .required(true)
            .defaultValue("32")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles processed successfully")
            .build();

    private static final List<PropertyDescriptor> descriptors = List.of(RANDOM_STRING_LENGTH);

    private static final Set<Relationship> relationships = Set.of(SUCCESS);

    private static final RandomStringGenerator randomStringGenerator = RandomStringGenerator.builder().withinRange('A', 'Z').get();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int randomStringLength = context.getProperty(RANDOM_STRING_LENGTH).evaluateAttributeExpressions(flowFile).asInteger();
        final String generated = randomStringGenerator.generate(randomStringLength);
        final byte[] generatedBytes = generated.getBytes(StandardCharsets.US_ASCII);

        final FlowFile transformedFlowFile = session.write(flowFile, (inputStream, outputStream) -> {
            outputStream.write(generatedBytes);
            inputStream.transferTo(outputStream);
        });

        session.transfer(transformedFlowFile, SUCCESS);
    }
}
