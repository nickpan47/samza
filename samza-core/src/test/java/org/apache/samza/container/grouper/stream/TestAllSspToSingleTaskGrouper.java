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

package org.apache.samza.container.grouper.stream;

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestAllSspToSingleTaskGrouper {
  private static final String PROCESSOR_ID = "processor.id";

  @Test
  public void testGrouperCreatesTasknameWithProcessorId() {
    Map<String, String> configMap = new HashMap<String, String>() {
      {
        put(PROCESSOR_ID, "1234");
      }
    };

    Set<SystemStreamPartition> sspSet = new HashSet<SystemStreamPartition>() {
      {
        add(new SystemStreamPartition("test-system", "test-stream-1", new Partition(0)));
        add(new SystemStreamPartition("test-system", "test-stream-1", new Partition(1)));
        add(new SystemStreamPartition("test-system", "test-stream-2", new Partition(0)));
        add(new SystemStreamPartition("test-system", "test-stream-2", new Partition(1)));
      }
    };
    SystemStreamPartitionGrouper sspGrouper =
        new AllSspToSingleTaskGrouperFactory().getSystemStreamPartitionGrouper(new MapConfig(configMap));

    Map<TaskName, Set<SystemStreamPartition>> result = sspGrouper.group(sspSet);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());

    TaskName taskName = (TaskName) result.keySet().toArray()[0];
    Assert.assertTrue(
        "Task & container is synonymous in this grouper. Cannot find processor ID in the TaskName!",
        taskName.getTaskName().contains("1234"));
  }

}
