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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreams;
import org.apache.samza.task.StreamTask;


/**
 * Interface to be implemented by physical execution engine to deploy the config and jobs to run the {@link org.apache.samza.operators.MessageStreams}
 */
@InterfaceStability.Unstable
public interface ExecutionEnvironment {

  static ExecutionEnvironment getLocalEnvironment(Config config) {
    return null;
  }

  static ExecutionEnvironment fromConfig(Config config) { return null; }

  MessageStreams createGraph();

  /**
   * Method to be invoked to deploy and run the actual Samza jobs to execute {@link org.apache.samza.operators.MessageStreams}
   *
   * @param graph  the user-defined operator DAG
   */
  void run(MessageStreams graph);

  /**
   * Method to run specific {@link StreamTask} class. This is for backward compatible support and for programmers who really
   * want to control the physical {@link SystemStreamPartition} level programming logic.
   *
   * @param streamTask  the task to be instantiated in a single {@link org.apache.samza.job.StreamJob} and executed
   */
  void runTask(StreamTask streamTask, Config config);
}
