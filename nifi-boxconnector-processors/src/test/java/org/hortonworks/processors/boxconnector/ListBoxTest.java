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
    public void testProcessor() {

    }

    @Test
    public void testListBoxFolder() throws Exception {
        testRunner.setProperty(ListBox.INPUT_DIRECTORY_ID, "10");
        testRunner.setProperty(ListBox.DEVELOPER_TOKEN, "10");
        testRunner.run();
        testRunner.assertTransferCount(ListBox.REL_SUCCESS, 2);
    }

    private class ListBoxWithMockApi extends ListBox {
        @Override
        protected BoxFolder getFolder(ProcessContext context) {
            String token = context.getProperty(DEVELOPER_TOKEN).toString();
            BoxAPIConnection api = new BoxAPIConnection(token);
            return new MockBoxFolder(api, token);
        }
    }

    private class MockBoxFolder extends BoxFolder {
        public MockBoxFolder(BoxAPIConnection api, String dirId){
            super(api, dirId);
        }

        @Override
        public Iterator<BoxItem.Info> iterator() {
            List<BoxItem.Info> files = new ArrayList<>();

            BoxItem.Info bi_null = new BoxItem.Info() {
                // Not sure why I have to override this method. Appears to work fine for testing without it
                @Override
                public BoxResource getResource() {
                    return null;
                };
            };
            files.add(bi_null);

            BoxItem.Info b1 = new BoxItem.Info() {
                // Not sure why I have to override this method. Appears to work fine for testing without it
                @Override
                public BoxResource getResource() {
                    return null;
                };

                @Override
                public Date getModifiedAt() {
                    return new Date(1485786189);
                }

                @Override
                public long getSize() {
                    return 10l;
                }

                @Override
                public String getName() {
                    return "b1";
                }
            };
            files.add(b1);

            BoxItem.Info b2 = new BoxItem.Info() {
                // Not sure why I have to override this method. Appears to work fine for testing without it
                @Override
                public BoxResource getResource() {
                    return null;
                };

                @Override
                public Date getModifiedAt() {
                    return new Date(1485786190);
                }

                @Override
                public long getSize() {
                    return 11l;
                }

                @Override
                public String getName() {
                    return super.getName();
                }
            };
            files.add(b2);
            return files.iterator();
        }
    }
}