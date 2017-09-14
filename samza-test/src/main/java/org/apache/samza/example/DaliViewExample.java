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
import java.util.ArrayList;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplications;
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
import org.apache.samza.util.CommandLine;


/**
 * Example code to implement window-based counter
 */
public class DaliViewExample {

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

    StreamApplication app = StreamApplications.createStreamApp(config);
    app.openInput(input)
        .window(Windows.<PageViewEvent, String, Integer>keyedTumblingWindow(m -> m.memberId, Duration.ofSeconds(10), () -> 0, (m, c) -> c + 1)
            .setEarlyTrigger(Triggers.repeat(Triggers.count(5)))
            .setAccumulationMode(AccumulationMode.DISCARDING))
        .map(PageViewCount::new)
        .sendTo(app.openOutput(output, m -> m.memberId));

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
