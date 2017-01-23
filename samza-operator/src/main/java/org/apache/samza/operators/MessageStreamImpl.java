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

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> map(Function<M, TM> mapFn) {
    return this.map(new MapFunction<M, TM>() {
      @Override public void init(Config config, TaskContext context) {
      }

      @Override public TM apply(M message) {
        return mapFn.apply(message);
      }
    });
  }

  @Override
  public <TM extends MessageEnvelope> MessageStream<TM> flatMap(Function<M, Collection<TM>> flatMapFn) {
    return this.flatMap(new FlatMapFunction<M, TM>() {
      @Override public void init(Config config, TaskContext context) {
      }

      @Override public Collection<TM> apply(M message) {
        return flatMapFn.apply(message);
      }
    });
  }

  @Override
  public <K, OM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope> MessageStream<RM> join(MessageStream<OM> otherStream,
      BiFunction<M, OM, RM> joinFn) {
    return this.join(otherStream, new JoinFunction<M, OM, RM>() {
      @Override public void init(Config config, TaskContext context) {
      }

      @Override public RM apply(M message, OM otherMessage) {
        return joinFn.apply(message, otherMessage);
      }
    });
  }

  @Override
  public MessageStream<M> filter(Function<M, Boolean> filterFn) {
    return this.filter(new FilterFunction<M>() {
      @Override public void init(Config config, TaskContext context) {
      }

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
        }, new MessageStreamImpl<>(this.graph), OperatorSpec.OpCode.MAP, this.graph.getNextOpId());
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
    }, new MessageStreamImpl<>(this.graph), OperatorSpec.OpCode.FILTER, this.graph.getNextOpId());
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
        // no duplicated init call to joinWithContext.init()
      }
    };

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
