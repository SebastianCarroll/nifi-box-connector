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

import com.box.sdk.*;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.nifi.processor.ProcessContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ListBoxTest {

    private TestRunner testRunner;
    private ProcessContext context;

    @Before
    public void init() {
        //testRunner = TestRunners.newTestRunner(ListBoxWithMockApi.class);
        ListBoxWithMockApi box = new ListBoxWithMockApi();
        testRunner = TestRunners.newTestRunner(box);
        context = testRunner.getProcessContext();
    }

    @Test
    public void testProcessor() {}

    @Test
    public void testListing() throws Exception {
        testRunner.setProperty(ListBox.INPUT_DIRECTORY_ID, "10");
        testRunner.setProperty(ListBox.DEVELOPER_TOKEN, "10");
        testRunner.run();
        testRunner.assertTransferCount(ListBox.REL_SUCCESS, 2);
    }

    @Test
    public void testReset() throws Exception {
        testRunner.setProperty(ListBox.INPUT_DIRECTORY_ID, "10");
        testRunner.setProperty(ListBox.DEVELOPER_TOKEN, "10");
        testRunner.run();
        testRunner.assertTransferCount(ListBox.REL_SUCCESS, 2);

        // No change in value, no list
        testRunner.clearTransferState();
        testRunner.setProperty(ListBox.INPUT_DIRECTORY_ID, "10");
        testRunner.run();
        testRunner.assertTransferCount(ListBox.REL_SUCCESS, 0);

        // A change, should list
        testRunner.clearTransferState();
        testRunner.setProperty(ListBox.INPUT_DIRECTORY_ID, "11");
        testRunner.run();
        testRunner.assertTransferCount(ListBox.REL_SUCCESS, 2);

        // No change, no list
        testRunner.clearTransferState();
        testRunner.run();
        testRunner.assertTransferCount(ListBox.REL_SUCCESS, 0);
    }

    @Test
    public void testAttributesCorrect() throws Exception {
        // TODO: Complete test
    }

    private class ListBoxWithMockApi extends ListBox {
        @Override
        protected BoxFolder getFolder(ProcessContext context) {
            String token = context.getProperty(DEVELOPER_TOKEN).toString();
            BoxAPIConnection api = new BoxAPIConnection(token);
            MockBoxFolder folder =  new MockBoxFolder(api, token);
            return folder
                    .add_null()
                    .add("b1", 10l, new Date(1485786189))
                    .add("b2", 11l, new Date(1485786190));
        }

    }

    private class MockBoxFolder extends BoxFolder {
        private List<BoxItem.Info> files;

        public MockBoxFolder(BoxAPIConnection api, String dirId){
            super(api, dirId);
            files = new ArrayList<>();
        }

        public MockBoxFolder add_null(){
            files.add( new BoxItem.Info() {
                @Override
                public BoxResource getResource() { return null; };
            });
            return this;
        }

        public MockBoxFolder add(String name, long size, Date mod){
            files.add ( new BoxItem.Info() {
                // Not sure why I have to override this method. Appears to work fine for testing without it
                @Override
                public BoxResource getResource() {
                    return null;
                };

                @Override
                public Date getModifiedAt() { return mod; }

                @Override
                public long getSize() { return size; }

                @Override
                public String getName() { return name; }
            } );
            return this;
        }

        @Override
        public Iterator<BoxItem.Info> iterator() { return files.iterator(); }
    }
}