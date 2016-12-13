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
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.system.SystemStream;

import java.util.*;


public class MessageStreamsBuilderImpl implements MessageStreamsBuilder {
  private final Map<SystemStream, MessageStreamImpl> inputStreamsMap = new HashMap<>();

  @Override public <M extends MessageEnvelope> MessageStream<M> addInputStream(SystemStream input) {
    inputStreamsMap.putIfAbsent(input, new MessageStreamImpl<>(this));
    return inputStreamsMap.get(input);
  }

  @Override public Map<SystemStream, MessageStream> getAllInputStreams() {
    return Collections.unmodifiableMap(inputStreamsMap);
  }

  public void swapInputStream(SystemStream ss, MessageStream<IncomingSystemMessageEnvelope> mergedStream) {
    if (!inputStreamsMap.containsKey(ss)) {
      throw new IllegalArgumentException("Requested SystemStream is not defined as input to this MessageStreamBuilder");
    }
    this.inputStreamsMap.get(ss).switchTo((MessageStreamImpl<IncomingSystemMessageEnvelope>) mergedStream);
    this.inputStreamsMap.put(ss, (MessageStreamImpl<IncomingSystemMessageEnvelope>) mergedStream);
  }

  private MessageStreamImpl cloneStream(MessageStreamImpl source, Map<MessageStreamImpl, MessageStreamImpl> clonedStreams) {
    if (clonedStreams.containsKey(source)) {
      // this has been cloned already
      return clonedStreams.get(source);
    }
    return source.getClone(this, clonedStreams);
  }

  public void clone(MessageStreamsBuilderImpl source) {
    Map<MessageStreamImpl, MessageStreamImpl> clonedStreams = new HashMap<>();
    source.inputStreamsMap.forEach((ss, ms) -> {
      this.inputStreamsMap.put(ss, this.cloneStream(ms, clonedStreams));
      clonedStreams.put(ms, this.inputStreamsMap.get(ss));
    });
  }
}
