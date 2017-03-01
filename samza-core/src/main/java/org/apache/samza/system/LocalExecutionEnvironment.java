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

import org.apache.samza.operators.StreamApplication;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraphImpl;


/**
 * This class implements the {@link ExecutionEnvironment} that runs the applications in standalone environment
 */
public class LocalExecutionEnvironment extends AbstractExecutionEnvironment {

  public LocalExecutionEnvironment(Config config) {
    super(config);
  }

  // TODO: may want to move this to a common base class for all {@link ExecutionEnvironment}
  StreamGraph createGraph(StreamApplication app, Config config) {
    StreamGraphImpl graph = new StreamGraphImpl();
    app.init(graph, config);
    return graph;
  }

  @Override public void run(StreamApplication app, Config config) {
    // 1. get logic graph for optimization
    // StreamGraph logicGraph = this.createGraph(app, config);
    // 2. potential optimization....
    // 3. create new instance of StreamApplication that would generate the optimized graph
    // 4. create all input/output/intermediate topics
    // 5. create the configuration for StreamProcessor
    // 6. start the StreamProcessor w/ optimized instance of StreamApplication
  }

  @Override
  public void start(StreamApplication graphBuilder, Config config) {

  }

  @Override
  public void stop() {

  }

}
