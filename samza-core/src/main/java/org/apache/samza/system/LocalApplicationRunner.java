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

import org.apache.samza.config.StreamProcessorConfig;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.task.TaskFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner extends AbstractApplicationRunner {
  private StreamProcessor streamProcessor = null;

  public LocalApplicationRunner(Config config) {
    super(config);
  }

  // TODO: may want to move this to a common base class for all {@link ApplicationRunner}
  StreamGraph createGraph(StreamApplication app, Config config) {
    StreamGraphImpl graph = new StreamGraphImpl();
    app.init(graph, config);
    return graph;
  }

  @Override
  public void start(StreamApplication graphBuilder, Config config) {

  }

  @Override
  public <T> void start(TaskFactory<T> taskFactory, Config config) {
    StreamProcessorConfig spConf = new StreamProcessorConfig(config);
    this.streamProcessor = new StreamProcessor(spConf.getProcessorId(), config, null, taskFactory);
  }

  @Override
  public void stop() {
    this.streamProcessor.stop();
  }

  @Override
  public boolean isRunning() throws Exception {
    return false;
  }

}
