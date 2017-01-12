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

import org.apache.samza.config.Config;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.*;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OperatorSpecs;
import org.apache.samza.operators.windows.Window;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.TaskContext;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;


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

  private final Class keySerde;

  private final Class msgSerde;

  /**
   * Default constructor
   *
   * @param graph the {@link MessageStreamGraphImpl} object that this stream belongs to
   */
  MessageStreamImpl(MessageStreamGraphImpl graph) {
    this(graph, null, null);
  }

  <K, V> MessageStreamImpl(MessageStreamGraphImpl graph, Serde<K> keySerde, Serde<V> msgSerde) {
    this.graph = graph;
    this.keySerde = keySerde != null ? keySerde.getClass() : null;
    this.msgSerde = msgSerde != null ? msgSerde.getClass() : null;
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> map(Function<M, TM> mapFn) {
    return this.map(new MapFunction<M, TM>() {
      @Override public void init(Config config, TaskContext context) { }

      @Override public TM apply(M message) {
        return mapFn.apply(message);
      }
    });
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> flatMap(Function<M, Collection<TM>> flatMapFn) {
    return this.flatMap(new FlatMapFunction<M, TM>() {
      @Override public void init(Config config, TaskContext context) { }

      @Override public Collection<TM> apply(M message) {
        return flatMapFn.apply(message);
      }
    });
  }

  @Override
  public <K, OM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope> MessageStream<RM> join(MessageStream<OM> otherStream,
      BiFunction<M, OM, RM> joinFn) {
    return this.join(otherStream, new JoinFunction<M, OM, RM>() {
      @Override public void init(Config config, TaskContext context) { }

      @Override public RM apply(M message, OM otherMessage) {
        return joinFn.apply(message, otherMessage);
      }
    });
  }

  @Override
  public MessageStream<M> filter(Function<M, Boolean> filterFn) {
    return this.filter(new FilterFunction<M>() {
      @Override public void init(Config config, TaskContext context) { }

      @Override public boolean apply(M message) {
        return filterFn.apply(message);
      }
    });
  }

  @Override public <TM extends MessageEnvelope> MessageStream<TM> map(MapFunction<M, TM> mapWithContext) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createStreamOperator(
        new FlatMapFunction<M, TM>() {
            @Override public Collection<TM> apply(M message) {
              return new ArrayList<TM>() {
                {
                  TM r = mapWithContext.apply(message);
                  if (r != null) {
                    this.add(r);
                  }
                }
              };
            }

            @Override public void init(Config config, TaskContext context) {
              mapWithContext.init(config, context);
            }
        }, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> flatMap(FlatMapFunction<M, TM> flatMapWithContext) {
    OperatorSpec<TM> op = OperatorSpecs.<M, TM>createStreamOperator(flatMapWithContext, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override public MessageStream<M> filter(FilterFunction<M> filterWithContext) {
    OperatorSpec<M> op = OperatorSpecs.<M, M>createStreamOperator(new FlatMapFunction<M, M>() {
      @Override public Collection<M> apply(M message) {
        return new ArrayList<M>() {
          {
            if (filterWithContext.apply(message)) {
              this.add(message);
            }
          }
        };
      }

      @Override public void init(Config config, TaskContext context) {
        filterWithContext.init(config, context);
      }
    }, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  @Override
  public void sink(SinkFunction<M> sinkFn) {
    this.registeredOperatorSpecs.add(OperatorSpecs.createSinkOperator(sinkFn));
  }

  @Override public <K, V> void sink(StreamSpec streamSpec, Serde<K> keySerdeClazz, Serde<V> msgSerdeClazz) {
    this.sink((m, mc, tc) -> mc.send(new OutgoingMessageEnvelope(streamSpec.getSystemStream(), m.getKey(), m.getMessage())));
    this.graph.<K, V, M>addOutStream(streamSpec, keySerdeClazz, msgSerdeClazz);
  }

  @Override
  public <WK, WV, WM extends WindowOutput<WK, WV>> MessageStream<WM> window(
      Window<M, WK, WV, WM> window) {
    OperatorSpec<WM> wndOp = OperatorSpecs.createWindowOperator(
        window.getInternalWindowFn(), new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(wndOp);
    return wndOp.getOutputStream();
  }

  @Override public <K, OM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope> MessageStream<RM> join(
      MessageStream<OM> otherStream, JoinFunction<M, OM, RM> joinWithContext) {
    MessageStreamImpl<RM> outputStream = new MessageStreamImpl<>(this.graph);

    PartialJoinFunction<M, OM, RM> parJoin1 = new PartialJoinFunction<M, OM, RM>() {
      @Override public RM apply(M m1, OM om) {
        return joinWithContext.apply(m1, om);
      }

      @Override public void init(Config config, TaskContext context) {
        joinWithContext.init(config, context);
      }
    };

    PartialJoinFunction<OM, M, RM> parJoin2 = new PartialJoinFunction<OM, M, RM>() {
      @Override public RM apply(OM m1, M m) {
        return joinWithContext.apply(m, m1);
      }

      @Override public void init(Config config, TaskContext context) {
        joinWithContext.init(config, context);
      }
    };

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

  @Override
  public <K> MessageStream<M> keyedBy(Function<M, K> parKeyExtractor) {
    OperatorSpec<M> op = OperatorSpecs.createPartitionOperator(parKeyExtractor, new MessageStreamImpl<>(this.graph));
    this.registeredOperatorSpecs.add(op);
    return op.getOutputStream();
  }

  /**
   * Place holder for explicit physical stream creation API
   *
  @Override public <K, V> MessageStream<M> through(StreamSpec intStream, Serde<K> keySerdeClazz, Serde<V> msgSerdeClazz) {
    this.sink(
        (m, mc, tc) -> mc.send(new OutgoingMessageEnvelope(intStream.getSystemStream(), m.getKey(), m.getMessage())));
    return this.graph.<K, V, M>addIntStream(intStream, keySerdeClazz, msgSerdeClazz);
  }
  */

  /**
   * Gets the operator specs registered to consume the output of this {@link MessageStream}. This is an internal API and
   * should not be exposed to users.
   *
   * @return  a collection containing all {@link OperatorSpec}s that are registered with this {@link MessageStream}.
   */
  public Collection<OperatorSpec> getRegisteredOperatorSpecs() {
    return Collections.unmodifiableSet(this.registeredOperatorSpecs);
  }

  /**
   * Method to get the associated key {@link Serde} class name, if defined
   *
   * @return  key {@link Serde} class name if defined. Otherwise, null
   */
  public Class<Serde<?>> getKeySerde() {
    return (Class<Serde<?>>) this.keySerde;
  }

  /**
   * Method to get the associated message {@link Serde} class name, if defined
   *
   * @return  message {@link Serde} class name if defined. Otherwise, null
   */
  public Class<Serde<?>> getMsgSerde() {
    return (Class<Serde<?>>) this.msgSerde;
  }
}
