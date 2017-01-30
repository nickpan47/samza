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

package org.apache.samza.operators.spec;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.FlatMapFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;

import java.util.ArrayList;
import java.util.function.Function;


/**
 * Factory methods for creating {@link OperatorSpec} instances.
 */
public class OperatorSpecs {

  private OperatorSpecs() {}

  /**
   * Creates a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param output  the output {@link MessageStreamImpl} object
   * @param <M>  type of input {@link MessageEnvelope}
   * @param <OM>  type of output {@link MessageEnvelope}
   * @return  the {@link StreamOperatorSpec}
   */
  public static <M extends MessageEnvelope, OM extends MessageEnvelope> StreamOperatorSpec<M, OM> createStreamOperator(
      FlatMapFunction<M, OM> transformFn, MessageStreamImpl<OM> output, OperatorSpec.OpCode opCode, int opId) {
    return new StreamOperatorSpec<>(transformFn, output, opCode, opId);
  }

  /**
   * Creates a {@link SinkOperatorSpec}.
   *
   * @param sinkFn  the sink function
   * @param <M>  type of input {@link MessageEnvelope}
   * @return  the {@link SinkOperatorSpec}
   */
  public static <M extends MessageEnvelope> SinkOperatorSpec<M> createSinkOperator(SinkFunction<M> sinkFn, OperatorSpec.OpCode opCode, int opId) {
    return new SinkOperatorSpec<>(sinkFn, opCode, opId);
  }

  /**
   * Creates a {@link WindowOperatorSpec}.
   *
   * @param window the description of the window.
   * @param <M> the type of input {@link MessageEnvelope}
   * @param <K> the type of key in the {@link MessageEnvelope} in this {@link org.apache.samza.operators.MessageStream}. If a key is specified,
   *            results are emitted per-key
   * @param <WK> the type of key in the {@link WindowPane}
   * @param <WV> the type of value in the window
   * @param <WM> the type of output {@link WindowPane}
   * @return  the {@link WindowOperatorSpec}
   */

  public static <M extends MessageEnvelope, K, WK, WV, WM extends WindowPane<WK, WV>> WindowOperatorSpec<M, K, WK, WV, WM> createWindowOperatorSpec(
      WindowInternal<M, K, WV> window, MessageStreamImpl<WM> wndOutput, int opId) {
    return new WindowOperatorSpec<>(window, wndOutput, opId);
  }

  /**
   * Creates a {@link PartialJoinOperatorSpec}.
   *
   * @param partialJoinFn  the join function
   * @param joinOutput  the output {@link MessageStreamImpl}
   * @param <M>  type of input {@link MessageEnvelope}
   * @param <K>  type of join key
   * @param <JM>  the type of {@link MessageEnvelope} in the other join stream
   * @param <OM>  the type of {@link MessageEnvelope} in the join output
   * @return  the {@link PartialJoinOperatorSpec}
   */
  public static <M extends MessageEnvelope<K, ?>, K, JM extends MessageEnvelope<K, ?>, OM extends MessageEnvelope> PartialJoinOperatorSpec<M, K, JM, OM> createPartialJoinOperatorSpec(
      PartialJoinFunction<M, JM, OM> partialJoinFn, MessageStreamImpl<OM> joinOutput, int opId) {
    return new PartialJoinOperatorSpec<>(partialJoinFn, joinOutput, opId);
  }

  /**
   * Creates a {@link StreamOperatorSpec} with a merger function.
   *
   * @param mergeOutput  the output {@link MessageStreamImpl} from the merger
   * @param <M>  the type of input {@link MessageEnvelope}
   * @return  the {@link StreamOperatorSpec} for the merge
   */
  public static <M extends MessageEnvelope> StreamOperatorSpec<M, M> createMergeOperator(MessageStreamImpl<M> mergeOutput, int opId) {
    return new StreamOperatorSpec<M, M>(message ->
        new ArrayList<M>() {
          {
            this.add(message);
          }
        },
        mergeOutput, OperatorSpec.OpCode.MERGE, opId);
  }

  /**
   * Creates a {@link PartitionOperatorSpec} with a key extractor function.
   */
  public static <K, M extends MessageEnvelope> PartitionOperatorSpec<K, M> createPartitionOperator(Function<M, K> parKeyExtractor, MessageStreamImpl<M> output, int opId) {
    return new PartitionOperatorSpec<K, M>(parKeyExtractor, output, opId);
  }
}
