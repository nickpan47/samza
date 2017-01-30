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
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CommandLine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;


/**
 * Example {@link StreamApplication} code to test the API methods
 */
public class SiteSpeedExample implements StreamGraphFactory {

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

  class RealUserMonitoringEventMessage extends JsonIncomingSystemMessageEnvelope<MessageType> {
    RealUserMonitoringEventMessage(IncomingSystemMessageEnvelope ime) {
      super((String) ime.getKey(), (MessageType) ime.getMessage(), ime.getOffset(), ime.getSystemStreamPartition());
    }
  }

  class StandardizedUserMonitoringEventMessage extends JsonIncomingSystemMessageEnvelope<MessageType> {
    private final String stdEventType;

    StandardizedUserMonitoringEventMessage(RealUserMonitoringEventMessage rumEvent) {
      super(rumEvent.getKey(), rumEvent.getMessage(), rumEvent.getOffset(), rumEvent.getSystemStreamPartition());
      this.stdEventType = rumEvent.getMessage().joinFields.get(0);
    }

    StandardizedUserMonitoringEventMessage(JsonMessageEnvelope jsonEvent, String key) {
      super(key, jsonEvent.getMessage(), jsonEvent.getOffset(), jsonEvent.getSystemStreamPartition());
      this.stdEventType = jsonEvent.getMessage().joinFields.get(0);
    }
  }

  private boolean filterFunction(RealUserMonitoringEventMessage rumEvent) {
    return rumEvent.getMessage().joinFields.get(0).equals("event_type");
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  private Collection<JsonMessageEnvelope> getMessages(StandardizedUserMonitoringEventMessage sumEvent) {
    List<String> values = sumEvent.getMessage().joinFields;
    Collection<JsonMessageEnvelope> outMsgs = new ArrayList<>();
    values.forEach(val -> {
        MessageType newMsgType = new MessageType();
        newMsgType.joinKey = val;
        newMsgType.joinFields = new ArrayList<String>();
        newMsgType.joinFields.add(sumEvent.stdEventType);
        newMsgType.joinFields.add(val);
        outMsgs.add(new JsonMessageEnvelope(sumEvent.getKey(), newMsgType, sumEvent.getOffset(), sumEvent.getSystemStreamPartition()));
      });
    return outMsgs;
  }

  private String getPartitionKey(JsonMessageEnvelope jsonEvent) {
    return jsonEvent.getMessage().joinKey;
  }

  class SessionData {
    SessionData(StandardizedUserMonitoringEventMessage sumEvent) {

    }
  }

  class SessionStatistics {
    // compute average/max/min/histogram, etc.
    SessionStatistics(Collection<SessionData> data) {

    }
  }

  private JsonIncomingSystemMessageEnvelope<SessionStatistics> convertToStats(WindowOutput<String, Collection<SessionData>> wndOutput) {
    String sessionKey = wndOutput.getKey();
    Collection<SessionData> sessionData = wndOutput.getMessage();
    SessionStatistics statistics = computeStatistics(sessionData);
    return new JsonIncomingSystemMessageEnvelope<>(sessionKey, statistics, null, null);
  }

  private SessionStatistics computeStatistics(Collection<SessionData> sessionData) {
    return new SessionStatistics(sessionData);
  }

  /**
   * used by remote execution environment to launch the job in remote program. The remote program should follow the similar
   * invoking context as in standalone:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ExecutionEnvironment remoteEnv = ExecutionEnvironment.getRemoteEnvironment(config);
   *     remoteEnv.run(new UserMainExample(), config);
   *   }
   *
   */
  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);
    MessageStream<IncomingSystemMessageEnvelope> newSource = graph.createInStream(input1, null, null);
    MessageStream result = newSource.map(msg -> new RealUserMonitoringEventMessage(msg)).
        filter(this::filterFunction).map(msg -> new StandardizedUserMonitoringEventMessage(msg)).
        flatMap(msg -> getMessages(msg)).
//        sendThrough(graph.createIntStream(input2, new StringSerde("UTF-8"), new JsonSerde<MessageType>()),
//            this::getPartitionKey).
        repartitionedBy(this::getPartitionKey).
        map(msg -> new StandardizedUserMonitoringEventMessage(msg, (String) msg.getKey())).
        window(Windows.<StandardizedUserMonitoringEventMessage, String, SessionData>intoSessions(msg -> msg.getKey(),
            msg -> new SessionData(msg))).
        map(this::convertToStats).
        sendTo(graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<SessionStatistics>()));
    return graph;
  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new SiteSpeedExample(), config);
  }

}
