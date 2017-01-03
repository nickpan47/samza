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
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;

import java.util.*;


public class MessageStreamGraphImpl implements MessageStreamGraph {
  /**
   * Maps keeping all {@link SystemStream}s that are input and output of operators in {@link MessageStreamGraphImpl}
   */
  private final Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> inStreams = new HashMap<>();
  private final Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> outStreams = new HashMap<>();
  private final Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> intStreams = new HashMap<>();

  /**
   * {@link ExecutionEnvironment} is the actual deployment of physical processes running the job
   */
  private final ExecutionEnvironment runner;

  /**
   * Default constructor
   *
   * @param runner  the {@link ExecutionEnvironment} to run the {@link org.apache.samza.operators.MessageStreamGraphImpl}
   */
  MessageStreamGraphImpl(ExecutionEnvironment runner) {
    this.runner = runner;
  }

  /**
   * Helper function to convert the map to a map of {@link StreamSpec} to {@link MessageStream} that
   *
   * @param map  raw maps to be converted
   * @return  a map keyed by {@link StreamSpec}
   */
  private Set<StreamSpec> getStreamSpecMap(Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> map) {
    Map<StreamSpec, MessageStream> streamSpecMap = new HashMap<>();
    map.forEach((ss, entry) -> streamSpecMap.put(entry.getKey(), entry.getValue()));
    return Collections.unmodifiableSet(streamSpecMap.keySet());
  }

  @Override
  public <M extends MessageEnvelope> MessageStream<M> addInStream(StreamSpec streamSpec) {
    if (!this.inStreams.containsKey(streamSpec.getSystemStream())) {
      this.inStreams.putIfAbsent(streamSpec.getSystemStream(), new Entry<>(streamSpec, new MessageStreamImpl<M>(this)));
    }
    return (MessageStream<M>) this.inStreams.get(streamSpec.getSystemStream()).getValue();
  }

  @Override public Collection<StreamSpec> getInStreams() {
    return this.getStreamSpecMap(this.inStreams);
  }

  @Override public Collection<StreamSpec> getOutStreams() {
    return this.getStreamSpecMap(this.outStreams);
  }

  @Override public Collection<StreamSpec> getIntStreams() {
    return this.getStreamSpecMap(this.intStreams);
  }

  @Override public void run() {
    this.runner.run(this);
  }

  /**
   * Helper method to be accessed by implementation classes only
   *
   * @param systemStream  the {@link SystemStream}
   * @return  a {@link MessageStreamImpl} object corresponding to the {@code systemStream}
   */
  MessageStreamImpl getStreamByName(SystemStream systemStream) {
    if (this.inStreams.containsKey(systemStream)) {
      return this.inStreams.get(systemStream).getValue();
    }
    if (this.intStreams.containsKey(systemStream)) {
      return this.intStreams.get(systemStream).getValue();
    }
    return null;
  }

  /**
   * Helper method to be used by {@link MessageStreamImpl} class
   *
   * @param streamSpec  the {@link StreamSpec} object defining the {@link SystemStream} as the output
   * @param <M>  the type of {@link MessageEnvelope}s in the output {@link SystemStream}
   * @return  the {@link MessageStreamImpl} object
   */
  <M extends MessageEnvelope> MessageStreamImpl<M> addOutStream(StreamSpec streamSpec) {
    if (!this.outStreams.containsKey(streamSpec.getSystemStream())) {
      this.outStreams.putIfAbsent(streamSpec.getSystemStream(), new Entry<>(streamSpec, new MessageStreamImpl<>(this)));
    }
    return (MessageStreamImpl<M>) this.outStreams.get(streamSpec.getSystemStream()).getValue();
  }

  /**
   * Helper method to be used by {@link MessageStreamImpl} class
   *
   * @param streamSpec  the {@link StreamSpec} object defining the {@link SystemStream} as an intermediate {@link SystemStream}
   * @param <M>  the type of {@link MessageEnvelope}s in the output {@link SystemStream}
   * @return  the {@link MessageStreamImpl} object
   */
  <M extends MessageEnvelope> MessageStreamImpl<M> addIntStream(StreamSpec streamSpec) {
    if (!this.intStreams.containsKey(streamSpec.getSystemStream())) {
      this.intStreams.putIfAbsent(streamSpec.getSystemStream(), new Entry<>(streamSpec, new MessageStreamImpl<>(this)));
    }
    return this.intStreams.get(streamSpec.getSystemStream()).getValue();
  }
}
