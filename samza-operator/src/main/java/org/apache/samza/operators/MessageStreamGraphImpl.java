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
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.system.SystemStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class MessageStreamGraphImpl implements MessageStreamGraph {
  /**
   * Maps keeping all {@link SystemStream}s that are input and output of operators in {@link MessageStreamGraphImpl}
   */
  private final Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> inStreams = new HashMap<>();
  private final Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> outStreams = new HashMap<>();
  private final Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> intStreams = new HashMap<>();

  /**
   * Helper function to convert the map to a map of {@link StreamSpec} to {@link MessageStream} that
   *
   * @param map  raw maps to be converted
   * @return  a map keyed by {@link StreamSpec}
   */
  private Map<StreamSpec, MessageStream> getStreamSpecMap(Map<SystemStream, Entry<StreamSpec, MessageStreamImpl>> map) {
    Map<StreamSpec, MessageStream> streamSpecMap = new HashMap<>();
    map.forEach((ss, entry) -> streamSpecMap.put(entry.getKey(), entry.getValue()));
    return Collections.unmodifiableMap(streamSpecMap);
  }

  @Override
  public <K, V, M extends MessageEnvelope<K, V>> MessageStream<M> addInStream(StreamSpec streamSpec, Serde<K> keySerdeClazz, Serde<V> msgSerdeClazz) {
    if (!this.inStreams.containsKey(streamSpec.getSystemStream())) {
      this.inStreams.putIfAbsent(streamSpec.getSystemStream(), new Entry<>(streamSpec, new MessageStreamImpl<M>(this, keySerdeClazz, msgSerdeClazz)));
    }
    return (MessageStream<M>) this.inStreams.get(streamSpec.getSystemStream()).getValue();
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
  <K, V, M extends MessageEnvelope<K, V>> MessageStreamImpl<M> addOutStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.outStreams.containsKey(streamSpec.getSystemStream())) {
      this.outStreams.putIfAbsent(streamSpec.getSystemStream(), new Entry<>(streamSpec, new MessageStreamImpl<M>(this, keySerde, msgSerde)));
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
  <K, V, M extends MessageEnvelope<K, V>> MessageStreamImpl<M> addIntStream(StreamSpec streamSpec, Serde<K> keySerde, Serde<V> msgSerde) {
    if (!this.intStreams.containsKey(streamSpec.getSystemStream())) {
      this.intStreams.putIfAbsent(streamSpec.getSystemStream(), new Entry<>(streamSpec, new MessageStreamImpl<M>(this, keySerde, msgSerde)));
    }
    return (MessageStreamImpl<M>) this.intStreams.get(streamSpec.getSystemStream()).getValue();
  }
}
