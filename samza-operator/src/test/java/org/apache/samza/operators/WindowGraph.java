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

package org.apache.samza.operators;

import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.Properties;
import java.util.Set;


/**
 * Example implementation of a simple user-defined tasks w/ window operators
 *
 */
public class WindowGraph {
  class MessageType {
    String field1;
    String field2;
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {

    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  public StreamGraphImpl createStreamGraph(ExecutionEnvironment runtimeEnv, Set<SystemStreamPartition> inputs) {
    StreamGraphImpl graph = new StreamGraphImpl();
    BiFunction<JsonMessageEnvelope, Integer, Integer> maxAggregator = (m, c) -> c + 1;
    inputs.forEach(source -> graph.<Object, Object, IncomingSystemMessageEnvelope>createInStream(new StreamSpec() {
      @Override public SystemStream getSystemStream() {
        return source.getSystemStream();
      }

      @Override public Properties getProperties() {
        return null;
      }
    }, null, null).
        map(m1 -> new JsonMessageEnvelope(this.myMessageKeyFunction(m1), (MessageType) m1.getMessage(), m1.getOffset(),
            m1.getSystemStreamPartition())).window(Windows.tumblingWindow(Duration.ofMillis(200), maxAggregator)));

    return graph;
  }

  String myMessageKeyFunction(MessageEnvelope<Object, Object> m) {
    return m.getKey().toString();
  }

}
