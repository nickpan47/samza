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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.serializers.Serde;

import java.util.Map;


/**
 * Job-level programming interface to create an operator DAG and run in various different runtime environments.
 */
@InterfaceStability.Unstable
public interface MessageStreamGraph {

  <K, V, M extends MessageEnvelope<K ,V>> MessageStream<M> addInStream(StreamSpec streamSpec, Serde<K> keySerdeClazz, Serde<V> msgSerdeClazz);

  /**
   * Place holders for possible access methods needed to get the streams defined in the {@link MessageStreamGraph}
   */
   Map<StreamSpec, MessageStream> getInStreams();
   Map<StreamSpec, MessageStream> getOutStreams();
   Map<StreamSpec, MessageStream> getIntStreams();
}
