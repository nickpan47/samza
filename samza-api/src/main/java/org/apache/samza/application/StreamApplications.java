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

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;

/**
 * This class defines the methods to create different types of Samza application instances: 1) high-level end-to-end
 * {@link StreamApplication}; 2) task-level {@link StreamTaskApplication}; 3) task-level {@link AsyncStreamTaskApplication}
 */
public class StreamApplications {
  //TODO: add the static map of all created application instances from the user program
  private static final Map<String, ApplicationBase> USER_APPS = new HashMap<>();

  private StreamApplications() {

  }

  public static StreamApplication createStreamApp(Config config) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    StreamApplication app = new StreamApplication(runner, config);
    USER_APPS.put(app.getGlobalAppId(), app);
    return app;
  }

  public static StreamTaskApplication createStreamTaskApp(Config config, Class<? extends StreamTask> streamTaskClass) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    StreamTaskApplication app = new StreamTaskApplication(() -> {
      try {
        return streamTaskClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new SamzaException("Failed to instantiate StreamTask", e);
      }
    }, runner, config);
    USER_APPS.put(app.getGlobalAppId(), app);
    return app;
  }

  public static AsyncStreamTaskApplication createAsyncStreamTaskApp(Config config, Class<? extends AsyncStreamTask> asyncStreamTaskClass) {
    ApplicationRunner runner = ApplicationRunner.fromConfig(config);
    AsyncStreamTaskApplication app = new AsyncStreamTaskApplication(() -> {
      try {
        return asyncStreamTaskClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new SamzaException("Failed to instantiate AsyncStreamTask", e);
      }
    }, runner, config);
    USER_APPS.put(app.getGlobalAppId(), app);
    return app;
  }

}
