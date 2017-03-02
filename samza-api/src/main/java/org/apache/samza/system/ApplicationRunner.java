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

import java.lang.reflect.Constructor;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.AppConfig;
import org.apache.samza.config.ConfigException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskFactory;


/**
 * Interface to be implemented by physical execution engine to deploy the config and jobs to run the {@link org.apache.samza.operators.StreamGraph}
 */
@InterfaceStability.Unstable
public interface ApplicationRunner extends StreamProvider {

  /**
   * Static method to load the local standalone environment
   *
   * @param config  configuration passed in to initialize the Samza standalone process
   * @return  the standalone {@link ApplicationRunner} to run the user-defined stream applications
   */
  static ApplicationRunner getLocalRunner(Config config) {
    return null;
  }

  /**
   * Static method to load the non-standalone environment.
   *
   * @param config  configuration passed in to initialize the Samza processes
   * @return  the configure-driven {@link ApplicationRunner} to run the user-defined stream applications
   */
  static ApplicationRunner fromConfig(Config config) {
    AppConfig appConfig = new AppConfig(config);
    try {
      Class<?> environmentClass = Class.forName(appConfig.getAppRunnerClass());
      if (ApplicationRunner.class.isAssignableFrom(environmentClass)) {
        Constructor<?> constructor = environmentClass.getConstructor(Config.class); // *sigh*
        return (ApplicationRunner) constructor.newInstance(config);
      }
    } catch (Exception e) {
      throw new ConfigException(String.format("Problem in loading ApplicationRunner class %s", appConfig.getAppRunnerClass()), e);
    }
    throw new ConfigException(String.format(
        "Class %s does not implement interface ApplicationRunner properly",
        appConfig.getAppRunnerClass()));
  }

  /**
   * Method to start an {@link StreamApplication}
   */
  void start(StreamApplication streamApplication, Config config) throws Exception;

  /**
   * Method to start a Samza job w/ specified {@link TaskFactory}
   *
   * @param taskFactory  the {@link TaskFactory} to create actual task instances in a Samza job
   * @param config  the configuration object for the Samza job
   * @param <T>  the type of task to be instantiated
   */
  <T> void start(TaskFactory<T> taskFactory, Config config) throws Exception;

  /**
   * Method to stop the current running application.
   */
  void stop() throws Exception;

  /**
   * Method to check whether the application is still running
   */
  boolean isRunning() throws Exception;
}
