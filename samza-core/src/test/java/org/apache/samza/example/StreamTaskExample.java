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
package org.apache.samza.example;

import org.apache.samza.config.Config;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.system.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.util.CommandLine;


/**
 * Example {@link StreamApplication} code to test the API methods
 */
public class StreamTaskExample implements TaskFactory<StreamTask> {


  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    try {
      localRunner.start(new StreamTaskExample(), config);
      while(localRunner.isRunning()) {
        Thread.sleep(10000);
      }
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      localRunner.stop();
    }
  }

  @Override
  public StreamTask createInstance() {
    return new StreamTask() {
      @Override
      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

      }
    };
  }
}
