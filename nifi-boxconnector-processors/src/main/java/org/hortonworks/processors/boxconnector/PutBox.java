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
package org.hortonworks.processors.boxconnector;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxUser;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutBox extends AbstractProcessor {

    private ComponentLog logger;

    public static final PropertyDescriptor UPLOAD_DIRECTORY_ID = new PropertyDescriptor
            .Builder().name("UPLOAD_DIRECTORY_ID")
            .displayName("Upload Directory ID")
            .description("Directory to upload files to (Referenced by the Box ID)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor
            .Builder().name("FILENAME")
            .displayName("Filename")
            .description("Name of file to upload")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEVELOPER_TOKEN  = new PropertyDescriptor
            .Builder().name("DEVELOPER_TOKEN")
            .displayName("Developer Token")
            .description("Token used to connect to Box")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("All FlowFiles that are received are routed to success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Files that failed to be pulled from Box")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = context.getLogger();

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(UPLOAD_DIRECTORY_ID);
        descriptors.add(FILENAME);
        descriptors.add(DEVELOPER_TOKEN);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            String token = context.getProperty(DEVELOPER_TOKEN).toString();
            BoxAPIConnection api = new BoxAPIConnection(token);
            String folder_id = context.getProperty(UPLOAD_DIRECTORY_ID).evaluateAttributeExpressions(flowFile).getValue();
            String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
            BoxFolder folder = new BoxFolder(api, folder_id);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {
                    logger.info("Trying to upload file " + filename);
                    folder.uploadFile(inputStream, filename);
                }
            });

            session.transfer(flowFile, REL_SUCCESS);
        } catch(Exception e){
            // TODO: There must be a better way to get the stack trace to a printable string
            logger.error("Processing failed " + Arrays.stream(e.getStackTrace()).map(s -> s.toString()).collect(Collectors.joining(" ")));
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}