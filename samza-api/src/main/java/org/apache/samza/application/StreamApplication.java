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


/**
 * This class defines the base class for applications written in {@link org.apache.samza.operators.StreamGraph} API
 */
@InterfaceStability.Unstable
public interface StreamApplication {

  default void initApp(Config config) {};

//  default void initTask(Config config, TaskContext context) {};
//
//  /**
//   * Users are required to implement this abstract method to initialize the processing logic of the application, in terms
//   * of a DAG of {@link org.apache.samza.operators.MessageStream}s and operators
//   *
//   * @param graph  the empty {@link MessageStreams} to be initialized
//   * @param config  the {@link Config} of the application
//   */
//  void initGraph(MessageStreams graph, Config config);
//
  /**
   * This static method provides a way for remote execution environment (i.e. YARN and Mesos) to load the user-defined
   * {@link StreamApplication} class from configuration.
   *
   * <p>User can specify the {@link StreamApplication} class to be loaded in the configuration variable "job.stream.application.class"=org.apache.samza.application.MyStreamApplication.
   * And the following method class load the corresponding class and returns an instance of {@link StreamApplication}.</p>
   *
   *
   * @param config  the {@link Config} for the application
   * @return  an instance of {@link StreamApplication} according to the user defined sub-class of {@link StreamApplication}
   */
  static StreamApplication fromConfig(Config config) {
    // TODO: placeholder. Should load the class name from config and instantiate the application instance
    // TODO: add config var example that set the user-implemented {@link StreamApplication} class
    return null;
  }
}
