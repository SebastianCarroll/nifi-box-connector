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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
public class ListBox extends AbstractProcessor {

    public static final PropertyDescriptor INPUT_DIRECTORY = new PropertyDescriptor
            .Builder().name("INPUT_DIRECTORY")
            .displayName("Input Directory")
            .description("Directory to search for files")
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

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(INPUT_DIRECTORY);
        descriptors.add(DEVELOPER_TOKEN);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
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

        // Call box api to list files in folder given
        // Create flow file for each file found with the file path + name as attributes
        String token = context.getProperty(DEVELOPER_TOKEN).toString();
        BoxAPIConnection api = new BoxAPIConnection(token);
        BoxFolder folder = new BoxFolder(api, "16169135715");

        try {
            for (BoxItem.Info itemInfo : folder) {
                if (itemInfo instanceof BoxFile.Info) {
                    BoxFile.Info fileInfo = (BoxFile.Info) itemInfo;

                    FlowFile flowFile = session.create();
                    flowFile = session.putAttribute(flowFile, "path", getPath(fileInfo));
                    flowFile = session.putAttribute(flowFile, "filename", fileInfo.getName());
                    session.transfer(flowFile, REL_SUCCESS);
                }
            }
        } catch (Exception e) {
            // TODO: Handle Failure here
            e.printStackTrace();
            System.out.println("Folder not found");
        }
    }

    private String getPath(BoxFile.Info fileInfo){
        return fileInfo.getPathCollection()
                .stream()
                .map(folder -> folder.getName())
                .collect(Collectors.joining("/"));
    }
}
