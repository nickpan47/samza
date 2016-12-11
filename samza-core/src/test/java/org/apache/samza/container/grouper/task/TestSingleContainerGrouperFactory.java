/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.container.grouper.task;

import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TestSingleContainerGrouperFactory {
  private static final String PROCESSOR_ID = "processor.id";

  @Test(expected = ConfigException.class)
  public void testBuildThrowsExceptionOnMissingProcessorId() {
    SingleContainerGrouperFactory factory = new SingleContainerGrouperFactory();
    factory.build(new MapConfig());
  }

  @Test(expected = NumberFormatException.class)
  public void testBuildThrowsExceptionOnInvalidProcessorId() {
    SingleContainerGrouperFactory factory = new SingleContainerGrouperFactory();
    factory.build(new MapConfig(Collections.singletonMap(PROCESSOR_ID, "abc123")));
  }

  @Test
  public void testBuildSucceeds() {
    SingleContainerGrouperFactory factory = new SingleContainerGrouperFactory();
    TaskNameGrouper grouper = factory.build(new MapConfig(Collections.singletonMap(PROCESSOR_ID, "1")));
    Assert.assertNotNull(grouper);
    Assert.assertTrue(grouper instanceof SingleContainerGrouper);
  }
}
