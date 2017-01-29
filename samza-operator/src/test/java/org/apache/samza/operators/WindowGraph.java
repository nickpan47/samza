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
import org.apache.samza.operators.windows.TriggerBuilder;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

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

    inputs.forEach(input -> graph.<Object, Object, IncomingSystemMessageEnvelope>createInStream(new StreamSpec() {
      @Override public SystemStream getSystemStream() {
        return input.getSystemStream();
      }

      @Override public Properties getProperties() {
        return null;
      }
    }, null, null).
            map(m1 -> new JsonMessageEnvelope(this.myMessageKeyFunction(m1), (MessageType) m1.getMessage(),
                m1.getOffset(), m1.getSystemStreamPartition())).
            window(Windows.<JsonMessageEnvelope, String>intoSessionCounter(
                m -> String.format("%s-%s", m.getMessage().field1, m.getMessage().field2)).
                setTriggers(TriggerBuilder.<JsonMessageEnvelope, Integer>earlyTriggerWhenExceedWndLen(100).
                    addTimeoutSinceLastMessage(30000))).
        sendTo(graph.createOutStream(new StreamSpec() {
          @Override public SystemStream getSystemStream() {
            return null;
          }

          @Override public Properties getProperties() {
            return null;
          }
        }, new StringSerde("UTF-8"), new IntegerSerde())));

    return graph;
  }

  String myMessageKeyFunction(MessageEnvelope<Object, Object> m) {
    return m.getKey().toString();
  }

}
