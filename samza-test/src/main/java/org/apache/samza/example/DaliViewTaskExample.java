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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplications;
import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.dali.DaliStreamFactory;
import org.apache.samza.system.kafka.KafkaStreamFactory;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.CommandLine;


/**
 * Example code to implement window-based counter
 */
public class DaliViewTaskExample {

  static class MyDaliStreamTask implements StreamTask {
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
      // Implement user logic here
      return;
    };
  }


  // local execution mode
  public static void main(String[] args) throws IOException {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    DaliStreamFactory daliFactory = DaliStreamFactory.create("dali").withViewMetadata("url://viewmetastore");
    KafkaStreamFactory kafkaFactory = KafkaStreamFactory.create("kafka").withBootstrapServers("localhost:9092");

    StreamDescriptor.Input<String, PageViewEvent> input =
        daliFactory.<String, PageViewEvent>getInputStreamDescriptor("DaliPageViewEevent");
    StreamDescriptor.Output<String, PageViewCount> output =
        kafkaFactory.<String, PageViewCount>getOutputStreamDescriptor("pageViewEventPerMemberStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>());

    StreamTaskApplication app = StreamApplications.createStreamTaskApp(config, MyDaliStreamTask.class);
    app.addInputs(Collections.singletonList(input)).addOutputs(Collections.singletonList(output));

    app.run();
    app.waitForFinish();
  }

  class PageViewEvent {
    String pageId;
    String memberId;
    long timestamp;

    PageViewEvent(String pageId, String memberId, long timestamp) {
      this.pageId = pageId;
      this.memberId = memberId;
      this.timestamp = timestamp;
    }
  }

  static class PageViewCount {
    String memberId;
    long timestamp;
    int count;

    PageViewCount(WindowPane<String, Integer> m) {
      this.memberId = m.getKey().getKey();
      this.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.count = m.getMessage();
    }
  }
}
