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

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public class StreamGraphImpl implements StreamGraph {

  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} added to transform the {@link MessageEnvelope}
   * in the input {@link MessageStream}s.
   */
  private int opId = 0;

  private class SystemMessageStream<K, V, M extends MessageEnvelope<K, V>> extends MessageStreamImpl<M> {
    private final StreamSpec spec;
    private final Serde<K> keySerde;
    private final Serde<V> msgSerde;

    /**
     * Default constructor
     *

     * @param graph the {@link StreamGraphImpl} object that this stream belongs to
     */
    SystemMessageStream(StreamGraphImpl graph, StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
      super(graph);
      this.spec = streamSpec;
      this.keySerde = keySerde;
      this.msgSerde = msgSerde;
    }

    <PK> void send(M message, MessageCollector mc, TaskCoordinator tc, Function<M, PK> parKeyFunction) {
      // TODO: need to find a way to directly pass in the serde class names
      // mc.send(new OutgoingMessageEnvelope(this.spec.getSystemStream(), this.keySerde.getClass().getName(), this.msgSerde.getClass().getName(),
      //    message.getKey(), message.getKey(), message.getMessage()));
      if (parKeyFunction != null) {
        mc.send(new OutgoingMessageEnvelope(this.spec.getSystemStream(), parKeyFunction.apply(message), message.getKey(), message.getMessage()));
        return;
      }
      mc.send(new OutgoingMessageEnvelope(this.spec.getSystemStream(), message.getKey(), message.getMessage()));
    }

    StreamSpec getStreamSpec() {
      return this.spec;
    }
  }

  /**
   * Maps keeping all {@link SystemStream}s that are input and output of operators in {@link StreamGraphImpl}
   */
  private final Map<SystemStream, SystemMessageStream> inStreams = new HashMap<>();
  private final Map<SystemStream, SystemMessageStream> outStreams = new HashMap<>();
  private final Map<SystemStream, SystemMessageStream> intStreams = new HashMap<>();

  /**
   * Helper function to convert the map to a map of {@link StreamSpec} to {@link MessageStream} that
   *
   * @param map  raw maps to be converted
   * @return  a map keyed by {@link StreamSpec}
   */
  private Map<StreamSpec, MessageStream> getStreamSpecMap(Map<SystemStream, SystemMessageStream> map) {
    Map<StreamSpec, MessageStream> streamSpecMap = new HashMap<>();
    map.forEach((ss, entry) -> streamSpecMap.put(entry.getStreamSpec(), entry));
    return Collections.unmodifiableMap(streamSpecMap);
  }

  @Override
  public <K, V, M extends MessageEnvelope<K, V>> MessageStream<M> createInStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.inStreams.containsKey(streamSpec.getSystemStream())) {
      this.inStreams.putIfAbsent(streamSpec.getSystemStream(), new SystemMessageStream<K, V, M>(this, streamSpec, keySerde, msgSerde));
    }
    return (MessageStream<M>) this.inStreams.get(streamSpec.getSystemStream());
  }

  /**
   * Helper method to be used by {@link MessageStreamImpl} class
   *
   * @param streamSpec  the {@link StreamSpec} object defining the {@link SystemStream} as the output
   * @param <M>  the type of {@link MessageEnvelope}s in the output {@link SystemStream}
   * @return  the {@link MessageStreamImpl} object
   */
  @Override
  public <K, V, M extends MessageEnvelope<K, V>> MessageStream<M> createOutStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.outStreams.containsKey(streamSpec.getSystemStream())) {
      this.outStreams.putIfAbsent(streamSpec.getSystemStream(), new SystemMessageStream<K, V, M>(this, streamSpec, keySerde, msgSerde));
    }
    return (MessageStreamImpl<M>) this.outStreams.get(streamSpec.getSystemStream());
  }

  /**
   * Helper method to be used by {@link MessageStreamImpl} class
   *
   * @param streamSpec  the {@link StreamSpec} object defining the {@link SystemStream} as an intermediate {@link SystemStream}
   * @param <M>  the type of {@link MessageEnvelope}s in the output {@link SystemStream}
   * @return  the {@link MessageStreamImpl} object
   */
  @Override
  public <K, V, M extends MessageEnvelope<K, V>> MessageStreamImpl<M> createIntStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.intStreams.containsKey(streamSpec.getSystemStream())) {
      this.intStreams.putIfAbsent(streamSpec.getSystemStream(), new SystemMessageStream<K, V, M>(this, streamSpec, keySerde, msgSerde));
    }
    return (MessageStreamImpl<M>) this.intStreams.get(streamSpec.getSystemStream());
  }

  @Override public Map<StreamSpec, MessageStream> getInStreams() {
    return this.getStreamSpecMap(this.inStreams);
  }

  @Override public Map<StreamSpec, MessageStream> getOutStreams() {
    return this.getStreamSpecMap(this.outStreams);
  }

  @Override public Map<StreamSpec, MessageStream> getIntStreams() {
    return this.getStreamSpecMap(this.intStreams);
  }

  /**
   * Helper method to get the {@link SystemMessageStream} corresponding to the input {@code messageStream}
   *
   * @param messageStream  the {@link MessageStream} object to be looked up in all output and intermediate {@link SystemMessageStream}s
   * @param <K>  the type of key in the {@link SystemMessageStream}
   * @param <V>  the type of message in the {@link SystemMessageStream}
   * @param <M>  the type of {@link MessageEnvelope} in the {@link SystemMessageStream}
   * @return  the {@link SystemMessageStream} object
   */
  private <K, V, M extends MessageEnvelope<K, V>> SystemMessageStream<K, V, M> getOutputStream(MessageStream<M> messageStream) {
    if (this.outStreams.containsValue(messageStream)) {
      return (SystemMessageStream<K, V, M>) messageStream;
    }
    if (this.intStreams.containsValue(messageStream)) {
      return (SystemMessageStream<K, V, M>) messageStream;
    }
    return null;
  }

  /**
   * Helper method to get the {@link SinkFunction} for a specific output/intermediate {@link SystemMessageStream}
   *
   * @param stream  the corresponding {@link MessageStream} to get create the {@link SinkFunction}
   * @param parKeyFunction  the function to extract partition key from the input {@link MessageEnvelope}, if not null.
   * @param <K>  the type of key in the {@link MessageEnvelope} in {@code stream}
   * @param <V>  the type of message in the {@link MessageEnvelope} in {@code stream}
   * @param <M>  the type of {@link MessageEnvelope} in {@code stream}
   * @param <PK>  the type of partition key in the {@link MessageEnvelope} in {@code stream}
   * @return  the {@link SinkFunction} to send a message to the corresponding output {@link SystemMessageStream}
   */
  <K, V, M extends MessageEnvelope<K, V>, PK> SinkFunction<M> getSinkFunction(MessageStream<M> stream, Function<M, PK> parKeyFunction) {
    SystemMessageStream<K, V, M> ostream = this.getOutputStream(stream);

    if (ostream == null) {
      throw new IllegalArgumentException(String.format("The input parameter: %s is not an system stream defined in the graph", stream));
    }

    return (M message, MessageCollector messageCollector, TaskCoordinator taskCoordinator) ->
        ostream.send(message, messageCollector, taskCoordinator, parKeyFunction);
  }

  int getNextOpId() {
    return this.opId++;
  }

  /**
   * Helper method to be accessed by {@link StreamOperatorTask}
   *
   * @param systemStream  the {@link SystemStream}
   * @return  a {@link MessageStreamImpl} object corresponding to the {@code systemStream}
   */
  public MessageStreamImpl getStreamByName(SystemStream systemStream) {
    if (this.inStreams.containsKey(systemStream)) {
      return this.inStreams.get(systemStream);
    }
    if (this.intStreams.containsKey(systemStream)) {
      return this.intStreams.get(systemStream);
    }
    return null;
  }

//  public void init(Config config, TaskContext context) {
//    // Assuming user-defined shared context is set in context
//    // this is to traverse the graph to initialize all the operator functions
//    Map<SystemStream, SystemMessageStream> inputStreams = new HashMap<>();
//    inputStreams.putAll(this.inStreams);
//    inputStreams.putAll(this.intStreams);
//    Set<MessageStream> allStreams = new HashSet<>();
//    inputStreams.forEach((sys, stream) -> {
//      if(!allStreams.contains(stream)) {
//        allStreams.add(stream);
//        Collection<OperatorSpec> ops = stream.getRegisteredOperatorSpecs();
//        ops.forEach(op -> initOperator(op, config, context, allStreams));
//      }
//    });
//  }
//
//  private void initOperator(OperatorSpec op, Config config, TaskContext context, Set<MessageStream> allStreams) {
//    op.init(config, context);
//    if (!allStreams.contains(op.getOutputStream())) {
//      allStreams.add(op.getOutputStream());
//      Collection<OperatorSpec> ops = op.getOutputStream().getRegisteredOperatorSpecs();
//      ops.forEach( nextOp -> initOperator(nextOp, config, context, allStreams));
//    }
//  }
}
