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

import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CommandLine;

import java.util.Properties;


public class OrderShipmentJoinExample implements StreamGraphFactory {

  /**
   * used by remote execution environment to launch the job in remote program. The remote program should follow the similar
   * invoking context as in standalone:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ExecutionEnvironment remoteEnv = ExecutionEnvironment.getRemoteEnvironment(config);
   *     UserMainExample runnableApp = new UserMainExample();
   *     runnableApp.run(remoteEnv, config);
   *   }
   *
   */
  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);

    MessageStream<OrderMessage> orders = graph.createInStream(input1, new StringSerde("UTF-8"), new JsonSerde<>());
    MessageStream<ShipmentMessage> shipments = graph.createInStream(input2, new StringSerde("UTF-8"), new JsonSerde<>());
    MessageStream<FulfilledOrderMessage> fulfilledOrders = graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>());

    orders.join(shipments, this::myJoinResult).sendTo(fulfilledOrders);

    return graph;
  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new OrderShipmentJoinExample(), config);
  }

  StreamSpec input1 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "Orders");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec input2 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "Shipment");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec output = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "FulfilledOrders");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  class OrderRecord {
    String orderId;
    long orderTimeMs;
  }

  class OrderMessage extends JsonIncomingSystemMessageEnvelope<OrderRecord> {
    OrderMessage(OrderRecord data, Offset offset, SystemStreamPartition partition) {
      super(data.orderId, data, offset, partition);
    }
  }

  class ShipmentRecord {
    String orderId;
    long shipTimeMs;
  }

  class ShipmentMessage extends JsonIncomingSystemMessageEnvelope<ShipmentRecord> {
    ShipmentMessage(ShipmentRecord data, Offset offset, SystemStreamPartition partition) {
      super(data.orderId, data, offset, partition);
    }
  }

  class FulFilledOrderRecord {
    String orderId;
    long orderTimeMs;
    long shipTimeMs;
  }

  class FulfilledOrderMessage extends JsonIncomingSystemMessageEnvelope<FulFilledOrderRecord> {
    FulfilledOrderMessage(FulFilledOrderRecord data, Offset offset, SystemStreamPartition partition) {
      super(data.orderId, data, offset, partition);
    }
  }

  FulfilledOrderMessage myJoinResult(OrderMessage m1, ShipmentMessage m2) {
    FulFilledOrderRecord joinRecord = new FulFilledOrderRecord();
    joinRecord.orderId = m1.getMessage().orderId;
    joinRecord.orderTimeMs = m1.getMessage().orderTimeMs;
    joinRecord.shipTimeMs = m2.getMessage().shipTimeMs;
    return new FulfilledOrderMessage(joinRecord, null, null);
  }

}
