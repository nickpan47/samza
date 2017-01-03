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
import org.apache.samza.operators.windows.WindowState;
import org.apache.samza.system.OutgoingMessageEnvelope;

import java.util.*;
import java.util.function.BiFunction;


/**
 * The implementation for input/output {@link MessageStream}s to/from the operators.
 * Users use the {@link MessageStream} API methods to describe and chain the operators specs.
 *
 * @param <M>  type of {@link MessageEnvelope}s in this {@link MessageStream}
 */
public class MessageStreamImpl<M extends MessageEnvelope> implements MessageStream<M> {
  /**
   * The {@link MessageStreamGraphImpl} object that contains this {@link MessageStreamImpl}
   */
  private final MessageStreamGraphImpl graph;

  /**
   * The set of operators that consume the {@link MessageEnvelope}s in this {@link MessageStream}
   */
  private final Set<OperatorSpec> registeredOperatorSpecs = new HashSet<>();

  /**
   * Default constructor
   *
   * @param graph the {@link MessageStreamGraphImpl} object that this stream belongs to
   */
  public MessageStreamImpl(MessageStreamGraphImpl graph) {
    this.graph = graph;
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> map(MapFunction<M, TM> mapFn) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createStreamOperator(
        m -> new ArrayList<TM>() {
          {
            TM r = mapFn.apply(m);
            if (r != null) {
              this.add(r);
            }
          }
        }, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapFn) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createStreamOperator(flatMapFn, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public MessageStream<M> filter(FilterFunction<M> filterFn) {
    OperatorSpec<M> op = OperatorSpecs.<M, M>createStreamOperator(
        t -> new ArrayList<M>() {
          {
            if (filterFn.apply(t)) {
              this.add(t);
            }
          }
        }, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public void sink(SinkFunction<M> sinkFn) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSinkOperator(sinkFn));
  }

  @Override public void sink(StreamSpec streamSpec) {
    this.sink((m, mc, tc) -> mc.send(new OutgoingMessageEnvelope(streamSpec.getSystemStream(), m.getKey(), m.getMessage())));
    this.graph.addOutStream(streamSpec);
  }

  @Override
  public <WK, WV, WM extends WindowOutput<WK, WV>> MessageStream<WM> window(
      Window<M, WK, WV, WM> window) {
    OperatorSpec<WM> wndOp = OperatorSpecs.<M, WK, WindowState<WV>, WM>createWindowOperator(
        window.getInternalWindowFn(), new MessageStreamImpl<WM>(this.graph));
    this.registeredOperatorSpecs.add(wndOp);
    return wndOp.getOutputStream();
  }

  @Override
  public <K, OM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope> MessageStream<RM> join(MessageStream<OM> otherStream,
      JoinFunction<M, OM, RM> joinFn) {
    MessageStreamImpl<RM> outputStream = new MessageStreamImpl<>(this.graph);

    BiFunction<M, OM, RM> parJoin1 = joinFn::apply;
    BiFunction<OM, M, RM> parJoin2 = (m, t1) -> joinFn.apply(t1, m);

    // TODO: need to add default store functions for the two partial join functions

    ((MessageStreamImpl<OM>) otherStream).registeredOperatorSpecs.add(
        OperatorSpecs.<OM, K, M, RM>createPartialJoinOperator(parJoin2, outputStream));
    this.registeredOperatorSpecs.add(OperatorSpecs.<M, K, OM, RM>createPartialJoinOperator(parJoin1, outputStream));
    return outputStream;
  }

  @Override
  public MessageStream<M> merge(Collection<MessageStream<M>> otherStreams) {
    MessageStreamImpl<M> outputStream = new MessageStreamImpl<>(this.graph);

    otherStreams.add(this);
    otherStreams.forEach(other -> ((MessageStreamImpl<M>) other).registeredOperatorSpecs
        .add(OperatorSpecs.createMergeOperator(outputStream)));
    return outputStream;
  }

  @Override public MessageStream<M> through(StreamSpec intStream) {
    this.sink((m, mc, tc) -> mc.send(new OutgoingMessageEnvelope(intStream.getSystemStream(), m.getKey(), m.getMessage())));
    return this.graph.addIntStream(intStream);
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
