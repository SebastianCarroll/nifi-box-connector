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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.FileInfo;
import org.apache.nifi.processors.standard.AbstractListProcessor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
public class ListBox extends AbstractListProcessor<FileInfo> {

    public static final PropertyDescriptor INPUT_DIRECTORY_ID = new PropertyDescriptor
            .Builder().name("INPUT_DIRECTORY_ID")
            .displayName("Input Directory ID")
            .description("Directory to search for files (Referenced by the Box ID")
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

    //////////////
    // Overridden abstract methods
    @Override
    protected Map<String, String> createAttributes(FileInfo fileInfo,
                                                   ProcessContext processContext) {
        // TODO: How to add the attributes here given I need info from the box folder
        return new HashMap<>();
    }

    @Override
    protected String getPath(ProcessContext processContext) {
        return null;
    }

    @Override
    protected List<FileInfo> performListing(ProcessContext context, Long minTimestamp) throws IOException {
        BoxFolder folder = getFolder(context);
        return listBoxFolder(folder);
    }

    @Override
    protected boolean isListingResetNecessary(PropertyDescriptor property) {
        return INPUT_DIRECTORY_ID.equals(property);
    }

    @Override
    protected Scope getStateScope(ProcessContext processContext) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }

    //////////////
    // Overridden Standard Methods
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(INPUT_DIRECTORY_ID);
        properties.add(DEVELOPER_TOKEN);
        return properties;
    }

    //////////////
    // Helper methods
    protected BoxFolder getFolder(ProcessContext context) {
        String token = context.getProperty(DEVELOPER_TOKEN).toString();
        BoxAPIConnection api = new BoxAPIConnection(token);
        return new BoxFolder(api, context.getProperty(INPUT_DIRECTORY_ID).toString());
    }

    private List<FileInfo> listBoxFolder(BoxFolder folder) {
        // TODO: This logger is meaningless - will just output an class and number
        this.getLogger().debug("Listing box folder: {}", new Object[]{folder});

        return StreamSupport
                // TODO: Do we want parallel := false?
                .stream(folder.spliterator(), false)
                // TODO: Probably should be a class method
                .map(file -> new FileConverter(file).build())
                .collect(Collectors.toList());
    }

    /**
     * Helper class to encapsulate the difficulties of the
     * BoxItem.Info class having null values
     */
    private class FileConverter {
        private BoxItem.Info file;

        public FileConverter(BoxItem.Info fileIn){
            file = fileIn;
        }

        public FileInfo build(){
            // TODO: Add logger in this class
            //this.getLogger().debug("Converted file: Name: {}, Size: {}, Modified: {} ",
             //       new Object[]{getName(), getSize(), getModTime()});

            return new FileInfo.Builder()
                    .filename(getName())
                    .size(getSize())
                    .lastModifiedTime(getModTime())
                    .build();
        }

        private String getName(){
            return file.getName();
        }

        private long getSize(){
            return file.getSize();
        }

        private long getModTime() {
            Date[] dates = {
                    file.getContentCreatedAt(),
                    file.getContentModifiedAt(),
                    file.getCreatedAt(),
                    file.getModifiedAt()};

            return Arrays.stream(dates)
                    .filter(d -> (d != null))
                    .map(d -> d.getTime())
                    .max(Long::compareTo)
                    .orElse(0l);
        }
    }
}
