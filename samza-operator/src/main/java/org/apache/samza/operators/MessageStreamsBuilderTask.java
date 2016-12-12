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

import org.apache.samza.Partition;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.util.*;


public class MessageStreamsBuilderTask implements StreamOperatorTask {
  private final Map<SystemStream, Map<Partition, MessageStream<IncomingSystemMessageEnvelope>>> inputBySystemStream = new HashMap<>();
  private final MessageStreamsBuilderImpl taskStreamBuilder;

  public MessageStreamsBuilderTask(MessageStreamsBuilder streamBuilder) {
    this.taskStreamBuilder = ((MessageStreamsBuilderImpl) streamBuilder).cloneTaskBuilder();
  }

  public void transform(Map<SystemStreamPartition, MessageStream<IncomingSystemMessageEnvelope>> streams) {
    // use {@code streamBuilder} as the template to instantiate the actual program
    streams.forEach((ssp, mstream) -> {
        this.inputBySystemStream.putIfAbsent(ssp.getSystemStream(), new HashMap<>());
        this.inputBySystemStream.get(ssp.getSystemStream()).putIfAbsent(ssp.getPartition(), mstream);
      });
    this.inputBySystemStream.forEach((ss, parMap) -> {
        merge(ss, parMap);
      });
  }

  private void merge(SystemStream ss, Map<Partition, MessageStream<IncomingSystemMessageEnvelope>> parMap) {
    // Here we will assume that the program is at {@link SystemStream} level. Hence, any two partitions from the same {@link SystemStream}
    // that are assigned (grouped) in the same task will be "merged" to the same operator instances that consume the {@link SystemStream}

    List<MessageStream<IncomingSystemMessageEnvelope>> moreInputs = new ArrayList<>();
    Iterator<MessageStream<IncomingSystemMessageEnvelope>> streamIterator = parMap.values().iterator();
    MessageStreamImpl<IncomingSystemMessageEnvelope> mergedStream = (MessageStreamImpl<IncomingSystemMessageEnvelope>) streamIterator.next();
    streamIterator.forEachRemaining(m -> moreInputs.add((MessageStreamImpl<IncomingSystemMessageEnvelope>) m));
    if (moreInputs.size() > 0) {
      mergedStream = (MessageStreamImpl<IncomingSystemMessageEnvelope>) mergedStream.merge(moreInputs);
    }
    // Now swap the input stream in taskStreamBuilder w/ the mergedStream
    this.taskStreamBuilder.swapInputStream(ss, mergedStream);
  }
}
