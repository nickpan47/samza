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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.OperatorBiFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.operators.stream.IntermediateMessageStreamImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.control.IOGraph;
import org.apache.samza.system.StreamSpec;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link StreamGraph} that provides APIs for accessing {@link MessageStream}s to be used to
 * create the DAG of transforms.
 */
public class StreamGraphImpl implements StreamGraph {

  /**
   * Unique identifier for each {@link org.apache.samza.operators.spec.OperatorSpec} in the graph.
   * Should only be accessed by {@link MessageStreamImpl} via {@link #getNextOpId()}.
   */
  private int opId = 0;

  // We use a LHM for deterministic order in initializing and closing operators.
  private final Map<StreamSpec, InputOperatorSpec> inputOperators = new LinkedHashMap<>();
  private final Map<StreamSpec, OutputStreamImpl> outputStreams = new LinkedHashMap<>();
  private final Config config;

  private IOSystem defaultSystem = null;
  private ContextManager contextManager = null;

  public StreamGraphImpl(Config config) {
    this.config = config;
  }

  @Override
  public <K, V, M> MessageStream<M> getInputStream(StreamDescriptor.Input<K, V> inputDescriptor,
                                                   OperatorBiFunction<? super K, ? super V, ? extends M> msgBuilder)
      throws IOException {

    List<MessageStream<M>> inputs = new ArrayList<>();
    inputDescriptor.getStreamSpecs().forEach(spec -> {
      if (inputOperators.containsKey(spec)) {
        throw new IllegalStateException("getInputStream() invoked multiple times " + "with the same streamId: " + inputDescriptor.getStreamId());
      }

      StreamSpec streamSpec = spec;
      InputOperatorSpec<K, V, M> opSpec = new InputOperatorSpec<K, V, M>(streamSpec, inputDescriptor.getReader(spec), msgBuilder, this.getNextOpId());
      inputOperators.put(streamSpec, opSpec);
      inputs.add(new MessageStreamImpl<>(this, opSpec));
    });

    return MessageStream.mergeAll(inputs);
  }

  @Override
  public <K, V, M> OutputStream<K, V, M> getOutputStream(StreamDescriptor.Output<K, V> outputDescriptor,
                                                         MapFunction<? super M, ? extends K> keyExtractor, MapFunction<? super M, ? extends V> msgExtractor) {
    if (outputStreams.containsKey(outputDescriptor.getStreamSpec())) {
      throw new IllegalStateException("getOutputStream() invoked multiple times "
          + "with the same streamId: " + outputDescriptor.getStreamId());
    }

    StreamSpec streamSpec = outputDescriptor.getStreamSpec();
    OutputStreamImpl<K, V, M> outputStrm = new OutputStreamImpl<K, V, M>(outputDescriptor, keyExtractor, msgExtractor);
    outputStreams.put(streamSpec, outputStrm);
    return outputStrm;
  }

  @Override
  public void setContextManager(ContextManager contextManager) {
    this.contextManager = contextManager;
  }

  @Override
  public void setDefaultSystem(IOSystem defaultSystem) {
    this.defaultSystem = defaultSystem;
  }

  /**
   * Internal helper for {@link MessageStreamImpl} to add an intermediate {@link MessageStream} to the graph.
   * An intermediate {@link MessageStream} is both an output and an input stream.
   *
   * @param streamName the name of the stream to be created. Will be prefixed with job name and id to generate the
   *                   logical streamId.
   * @param keyExtractor the {@link MapFunction} to extract the outgoing key from the intermediate message
   * @param msgExtractor the {@link MapFunction} to extract the outgoing message from the intermediate message
   * @param msgBuilder the {@link OperatorBiFunction} to convert the incoming key and message to a message
   *                   in the intermediate {@link MessageStream}
   * @param <K> the type of key in the intermediate message
   * @param <V> the type of message in the intermediate message
   * @param <M> the type of messages in the intermediate {@link MessageStream}
   * @return  the intermediate {@link MessageStreamImpl}
   */
  <K, V, M> IntermediateMessageStreamImpl<K, V, M> getIntermediateStream(String streamName,
      MapFunction<? super M, ? extends K> keyExtractor, MapFunction<? super M, ? extends V> msgExtractor,
      OperatorBiFunction<? super K, ? super V, ? extends M> msgBuilder) throws IOException {
    String streamId = String.format("%s-%s-%s",
        config.get(JobConfig.JOB_NAME()),
        config.get(JobConfig.JOB_ID(), "1"),
        streamName);

    StreamDescriptor.Input<K, V> inStrm = StreamDescriptor.<K, V>input(streamId)
        .from(this.defaultSystem);

    StreamDescriptor.Output<K, V> outStrm = StreamDescriptor.<K, V>output(streamId)
        .from(this.defaultSystem);

    // intermediate stream should only have one StreamSpec
    StreamSpec streamSpec = inStrm.getStreamSpecs().iterator().next();
    if (inputOperators.containsKey(streamSpec) || outputStreams.containsKey(streamSpec)) {
      throw new IllegalStateException("getIntermediateStream() invoked multiple times "
          + "with the same streamId: " + streamId);
    }
    inputOperators.put(streamSpec, new InputOperatorSpec<>(streamSpec, inStrm.getReader(streamSpec), msgBuilder, this.getNextOpId()));
    outputStreams.put(streamSpec, new OutputStreamImpl<>(outStrm, keyExtractor, msgExtractor));
    return new IntermediateMessageStreamImpl<K, V, M>(this, inputOperators.get(streamSpec), outputStreams.get(streamSpec));
  }

  public Map<StreamSpec, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(inputOperators);
  }

  public Map<StreamSpec, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(outputStreams);
  }

  public ContextManager getContextManager() {
    return this.contextManager;
  }

  /* package private */ int getNextOpId() {
    return this.opId++;
  }

  /**
   * Get all {@link OperatorSpec}s available in this {@link StreamGraphImpl}
   *
   * @return  all available {@link OperatorSpec}s
   */
  public Collection<OperatorSpec> getAllOperatorSpecs() {
    Collection<InputOperatorSpec> inputOperatorSpecs = inputOperators.values();
    Set<OperatorSpec> operatorSpecs = new HashSet<>();
    for (InputOperatorSpec inputOperatorSpec: inputOperatorSpecs) {
      operatorSpecs.add(inputOperatorSpec);
      doGetOperatorSpecs(inputOperatorSpec, operatorSpecs);
    }
    return operatorSpecs;
  }

  private void doGetOperatorSpecs(OperatorSpec operatorSpec, Set<OperatorSpec> specs) {
    Collection<OperatorSpec> registeredOperatorSpecs = operatorSpec.getRegisteredOperatorSpecs();
    for (OperatorSpec registeredOperatorSpec: registeredOperatorSpecs) {
      specs.add(registeredOperatorSpec);
      doGetOperatorSpecs(registeredOperatorSpec, specs);
    }
  }

  /**
   * Returns <tt>true</tt> iff this {@link StreamGraphImpl} contains a join or a window operator
   *
   * @return  <tt>true</tt> iff this {@link StreamGraphImpl} contains a join or a window operator
   */
  public boolean hasWindowOrJoins() {
    // Obtain the operator specs from the streamGraph
    Collection<OperatorSpec> operatorSpecs = getAllOperatorSpecs();

    Set<OperatorSpec> windowOrJoinSpecs = operatorSpecs.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.WINDOW || spec.getOpCode() == OperatorSpec.OpCode.JOIN)
        .collect(Collectors.toSet());

    return windowOrJoinSpecs.size() != 0;
  }

  public StreamSpec getStreamSpec(String streamId) {
    Map<String, StreamSpec> streamIdMap = inputOperators.entrySet().stream()
        .collect(HashMap::new, (c1, e) -> c1.put(e.getKey().getId(), e.getKey()), HashMap::putAll);
    if (streamIdMap.containsKey(streamId)) {
      return streamIdMap.get(streamId);
    }

    streamIdMap = outputStreams.entrySet().stream()
        .collect(HashMap::new, (c1, e) -> c1.put(e.getKey().getId(), e.getKey()), HashMap::putAll);
    if (streamIdMap.containsKey(streamId)) {
      return streamIdMap.get(streamId);
    }

    return null;
  }

  public IOGraph toIOGraph() {
    return IOGraph.buildIOGraph(this);
  }

}
