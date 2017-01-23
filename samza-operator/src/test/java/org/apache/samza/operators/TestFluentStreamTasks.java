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
package org.apache.samza.operators;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;

import org.apache.samza.operators.impl.OperatorImpl;
import org.apache.samza.system.SingleJobExecutionEnvironment;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit test for {@link StreamOperatorTask}
 */
public class TestFluentStreamTasks {

  private final WindowGraph userTask = new WindowGraph();

  private final BroadcastGraph splitTask = new BroadcastGraph();

  private final JoinGraph joinTask = new JoinGraph();

  private final Set<SystemStreamPartition> inputPartitions = new HashSet<SystemStreamPartition>() { {
      for (int i = 0; i < 4; i++) {
        this.add(new SystemStreamPartition("my-system", "my-topic1", new Partition(i)));
      }
    } };

  @Test
  public void testUserTask() throws Exception {
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getSystemStreamPartitions()).thenReturn(this.inputPartitions);
    SingleJobExecutionEnvironment sjEnv = new SingleJobExecutionEnvironment();
    StreamOperatorTask adaptorTask = new StreamOperatorTask(this.userTask.createStreamGraph(sjEnv, inputPartitions));
    Field pipelineMapFld = StreamOperatorTask.class.getDeclaredField("operatorGraph");
    pipelineMapFld.setAccessible(true);
    Map<SystemStreamPartition, OperatorImpl> pipelineMap =
        (Map<SystemStreamPartition, OperatorImpl>) pipelineMapFld.get(adaptorTask);

    adaptorTask.init(mockConfig, mockContext);
    assertEquals(pipelineMap.size(), 4);
    this.inputPartitions.forEach(partition -> {
        assertNotNull(pipelineMap.get(partition));
      });
  }

  @Test
  public void testSplitTask() throws Exception {
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getSystemStreamPartitions()).thenReturn(this.inputPartitions);
    SingleJobExecutionEnvironment sjEnv = new SingleJobExecutionEnvironment();
    StreamOperatorTask adaptorTask = new StreamOperatorTask(this.splitTask.createStreamGraph(sjEnv, inputPartitions));
    Field pipelineMapFld = StreamOperatorTask.class.getDeclaredField("operatorGraph");
    pipelineMapFld.setAccessible(true);
    Map<SystemStreamPartition, OperatorImpl> pipelineMap =
        (Map<SystemStreamPartition, OperatorImpl>) pipelineMapFld.get(adaptorTask);

    adaptorTask.init(mockConfig, mockContext);
    assertEquals(pipelineMap.size(), 4);
    this.inputPartitions.forEach(partition -> {
        assertNotNull(pipelineMap.get(partition));
      });
  }

  @Test
  public void testJoinTask() throws Exception {
    Config mockConfig = mock(Config.class);
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getSystemStreamPartitions()).thenReturn(this.inputPartitions);
    SingleJobExecutionEnvironment sjEnv = new SingleJobExecutionEnvironment();
    StreamOperatorTask adaptorTask = new StreamOperatorTask(this.joinTask.createStreamGraph(sjEnv, inputPartitions));
    Field pipelineMapFld = StreamOperatorTask.class.getDeclaredField("operatorGraph");
    pipelineMapFld.setAccessible(true);
    Map<SystemStreamPartition, OperatorImpl> pipelineMap =
        (Map<SystemStreamPartition, OperatorImpl>) pipelineMapFld.get(adaptorTask);

    adaptorTask.init(mockConfig, mockContext);
    assertEquals(pipelineMap.size(), 4);
    this.inputPartitions.forEach(partition -> {
        assertNotNull(pipelineMap.get(partition));
      });
  }

}
