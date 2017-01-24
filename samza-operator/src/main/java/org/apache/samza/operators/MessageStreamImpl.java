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
import org.apache.samza.operators.functions.*;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowOutput;

import java.util.*;
import java.util.function.Function;


/**
 * The implementation for input/output {@link MessageStream}s to/from the operators.
 * Users use the {@link MessageStream} API methods to describe and chain the operators specs.
 *
 * @param <M>  type of {@link MessageEnvelope}s in this {@link MessageStream}
 */
public class MessageStreamImpl<M extends MessageEnvelope> implements MessageStream<M> {
  /**
   * The {@link MessageStreamsImpl} object that contains this {@link MessageStreamImpl}
   */
  private final MessageStreamsImpl graph;

  /**
   * The set of operators that consume the {@link MessageEnvelope}s in this {@link MessageStream}
   */
  private final Set<OperatorSpec> registeredOperatorSpecs = new HashSet<>();

  /**
   * Default constructor
   *
   * @param graph the {@link MessageStreamsImpl} object that this stream belongs to
   */
  MessageStreamImpl(MessageStreamsImpl graph) {
    this.graph = graph;
  }

  @Override public <TM extends MessageEnvelope> MessageStream<TM> map(MapFunction<M, TM> mapFn) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createStreamOperator(
        message -> new ArrayList<TM>() {
          {
            TM r = mapFn.apply(message);
            if (r != null) {
              this.add(r);
            }
          }
        },
        new MessageStreamImpl<>(this.graph), OperatorSpec.OpCode.MAP, this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapWithContext) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createStreamOperator(flatMapWithContext, new MessageStreamImpl<>(this.graph),
        OperatorSpec.OpCode.FLAT_MAP, this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override public MessageStream<M> filter(FilterFunction<M> filterWithContext) {
    OperatorSpec<M> op = OperatorSpecs.<M, M>createStreamOperator(
        message -> new ArrayList<M>() {
          {
            if (filterWithContext.apply(message)) {
              this.add(message);
            }
          }
        },
        new MessageStreamImpl<>(this.graph), OperatorSpec.OpCode.FILTER, this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public void sink(SinkFunction<M> sinkFn) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSinkOperator(sinkFn, OperatorSpec.OpCode.SINK, this.graph.getNextOpId()));
  }

  @Override public void sendTo(MessageStream<M> stream) {
    this.sendTo(stream, null);
  }

  @Override public MessageStream<M> sendThrough(MessageStream<M> stream) {
    this.sendTo(stream, null);
    return stream;
  }

  @Override public <K> void sendTo(MessageStream<M> stream, Function<M, K> parKeyFunction) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSinkOperator(this.graph.getSinkFunction(stream, parKeyFunction),
        OperatorSpec.OpCode.SEND_TO, this.graph.getNextOpId()));
  }

  @Override public <K> MessageStream<M> sendThrough(MessageStream<M> stream, Function<M, K> parKeyFunction) {
    this.sendTo(stream, parKeyFunction);
    return stream;
  }

  @Override
  public <WK, WV, WM extends WindowOutput<WK, WV>> MessageStream<WM> window(
      Window<M, WK, WV, WM> window) {
    OperatorSpec<WM> wndOp = OperatorSpecs.createWindowOperator(
        window.getInternalWindowFn(), new MessageStreamImpl<>(this.graph), this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(wndOp);
    return wndOp.getOutputStream();
  }

  @Override public <K, OM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope> MessageStream<RM> join(
      MessageStream<OM> otherStream, JoinFunction<M, OM, RM> joinFn) {
    MessageStreamImpl<RM> outputStream = new MessageStreamImpl<>(this.graph);

    PartialJoinFunction<M, OM, RM> parJoin1 = (M m1, OM om) -> joinFn.apply(m1, om);
    PartialJoinFunction<OM, M, RM> parJoin2 = (OM m1, M m) -> joinFn.apply(m, m1);

    // TODO: need to add default store functions for the two partial join functions

    ((MessageStreamImpl<OM>) otherStream).registeredOperatorSpecs.add(
        OperatorSpecs.<OM, K, M, RM>createPartialJoinOperator(parJoin2, outputStream, this.graph.getNextOpId()));
    this.registeredOperatorSpecs.add(OperatorSpecs.<M, K, OM, RM>createPartialJoinOperator(parJoin1, outputStream, this.graph.getNextOpId()));
    return outputStream;
  }

  @Override
  public MessageStream<M> merge(Collection<MessageStream<M>> otherStreams) {
    MessageStreamImpl<M> outputStream = new MessageStreamImpl<>(this.graph);

    otherStreams.add(this);
    otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).registeredOperatorSpecs
        .add(OperatorSpecs.createMergeOperator(outputStream, this.graph.getNextOpId())));
    return outputStream;
  }

  @Override
  public <K> MessageStream<M> keyedBy(Function<M, K> parKeyExtractor) {
    OperatorSpec<M> op = OperatorSpecs.createPartitionOperator(parKeyExtractor, new MessageStreamImpl<>(this.graph), this.graph.getNextOpId());
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  /**
   * Gets the operator specs registered to consume the output of this {@link MessageStream}. This is an internal API and
   * should not be exposed to users.
   *
   * @return  a collection containing all {@link OperatorSpec}s that are registered with this {@link MessageStream}.
   */
  public Collection<OperatorSpec> getRegisteredOperatorSpecs() {
    return Collections.unmodifiableSet(this.registeredOperatorSpecs);
  }

}
