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

package org.apache.samza.system;

import org.apache.samza.operators.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.task.StreamTask;


/**
 * This class implements the {@link ExecutionEnvironment} that runs the applications in standalone environment
 */
public class StandaloneExecutionEnvironment implements ExecutionEnvironment {

  @Override public StreamGraph createGraph() {
    return new StreamGraphImpl();
  }

  @Override public void run(StreamGraphFactory app, Config config) {
    // 1. get logic graph for optimization
    StreamGraph logicGraph = app.create(config);
    // 2. potential optimization....
    // 3. create new instance of StreamGraphFactory that would generate the optimized graph
    // 4. create all input/output/intermediate topics
    // 5. create the configuration for StreamProcessor
    // 6. start the StreamProcessor w/ optimized instance of StreamGraphFactory
  }

  @Override public void runTask(StreamTask streamTask, Config config) {
    // 1. create all input/output/intermediate topics
    // 2. create the single job configuration, w/ task.class=streamTask.getClass().getName()
    // 3. start the StreamProcessor for the whole graph in the same JVM process
  }
}
