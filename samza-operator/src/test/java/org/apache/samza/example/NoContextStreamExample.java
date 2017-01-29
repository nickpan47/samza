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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Example {@link StreamApplication} code to test the API methods
 */
public class NoContextStreamExample implements StreamGraphFactory {

  StreamSpec input1 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "input1");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec input2 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "input2");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec intermediate = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "intermediate");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec output = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "output");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  class MessageType {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  private JsonMessageEnvelope getInputMessage(IncomingSystemMessageEnvelope ism) {
    return new JsonMessageEnvelope(
        ((MessageType) ism.getMessage()).joinKey,
        (MessageType) ism.getMessage(),
        ism.getOffset(),
        ism.getSystemStreamPartition());
  }

  JsonMessageEnvelope myJoinResult(JsonMessageEnvelope m1, JsonMessageEnvelope m2) {
    MessageType newJoinMsg = new MessageType();
    newJoinMsg.joinKey = m1.getKey();
    newJoinMsg.joinFields.addAll(m1.getMessage().joinFields);
    newJoinMsg.joinFields.addAll(m2.getMessage().joinFields);
    return new JsonMessageEnvelope(m1.getMessage().joinKey, newJoinMsg, null, null);
  }

  /**
   * used by remote execution environment to launch the job in remote program. The remote program should follow the similar
   * invoking context as in standalone:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ExecutionEnvironment remoteEnv = ExecutionEnvironment.fromConfig(config);  //TODO: Example config vars to indicate YARN
   *     remoteEnv.run(StreamApplication.fromConfig(config), config);
   *   }
   *
   */
  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);
    MessageStream<IncomingSystemMessageEnvelope> inputSource1 = graph.<Object, Object, IncomingSystemMessageEnvelope>createInStream(
        input1, null, null);
    MessageStream<IncomingSystemMessageEnvelope> inputSource2 = graph.<Object, Object, IncomingSystemMessageEnvelope>createInStream(
        input2, null, null);
    MessageStream<JsonIncomingSystemMessageEnvelope<MessageType>> intStream = graph.createOutStream(intermediate,
        new StringSerde("UTF-8"), new JsonSerde<>());
    MessageStream<JsonIncomingSystemMessageEnvelope<MessageType>> outStream = graph.createOutStream(output,
        new StringSerde("UTF-8"), new JsonSerde<>());

    inputSource1.map(this::getInputMessage).
        <String, JsonMessageEnvelope, JsonIncomingSystemMessageEnvelope<MessageType>>join(inputSource2.map(this::getInputMessage), this::myJoinResult, Duration.of()).
        sendTo(outStream);

    return graph;
  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new NoContextStreamExample(), config);
  }

}
