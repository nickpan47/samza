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
package org.apache.samza.application;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.task.*;


/**
 * This class defines the base class for applications written in {@link org.apache.samza.task.StreamTask} API
 */
@InterfaceStability.Unstable
public abstract class StreamTaskApplication implements StreamTask, InitableTask, WindowableTask, ClosableTask {

  /**
   * The default run method that runs the given {@link StreamTaskApplication} in the specified environment and config
   *
   * @param env  the {@link ExecutionEnvironment} the application is running in
   * @param config  the {@link Config} of the application
   */
  public final void run(ExecutionEnvironment env, Config config) {
    try {
      env.runTask(this, config);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override public void close() throws Exception {

  }

  @Override public void init(Config config, TaskContext context) throws Exception {

  }

  @Override public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

}
