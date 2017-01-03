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
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.impl.OperatorImpl;
import org.apache.samza.operators.impl.OperatorImpls;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import java.util.*;


/**
 * An {@link StreamTask} implementation that receives {@link IncomingSystemMessageEnvelope}s and propagates them
 * through the user's stream transformations defined in {@link MessageStreamGraphImpl} using the
 * {@link MessageStream} APIs.
 * <p>
 * This class brings all the operator API implementation components together and feeds the
 * {@link IncomingSystemMessageEnvelope}s into the transformation chains.
 * <p>
 * It accepts an instance of the user implemented DAG {@link MessageStreamGraphImpl} as input parameter of the constructor.
 * When its own {@link #init(Config, TaskContext)} method is called during startup, it creates a {@link MessageStreamImpl}
 * corresponding to each of its input {@link org.apache.samza.system.SystemStreamPartition}s. Each input {@link MessageStreamImpl}
 * will be corresponding to either an input stream or intermediate stream in {@link MessageStreamGraphImpl}.
 * <p>
 * Then, this task calls {@link OperatorImpls#createOperatorImpls(MessageStreamImpl, TaskContext)} for each of the input
 * {@link MessageStreamImpl}. This instantiates the {@link org.apache.samza.operators.impl.OperatorImpl} DAG
 * corresponding to the aforementioned {@link org.apache.samza.operators.spec.OperatorSpec} DAG and returns the
 * root node of the DAG, which this class saves.
 * <p>
 * Now that it has the root for the DAG corresponding to each {@link org.apache.samza.system.SystemStreamPartition}, it
 * can pass the message envelopes received in {@link StreamTask#process(IncomingMessageEnvelope, MessageCollector, TaskCoordinator)}
 * along to the appropriate root nodes. From then on, each {@link org.apache.samza.operators.impl.OperatorImpl} propagates
 * its transformed output to the next set of {@link org.apache.samza.operators.impl.OperatorImpl}s.
 */
public final class StreamOperatorAdaptorTask implements StreamTask, InitableTask, WindowableTask {

  /**
   * A mapping from each {@link SystemStream} to the root node of its operator chain DAG.
   */
  private final Map<SystemStream, OperatorImpl<IncomingSystemMessageEnvelope, ? extends MessageEnvelope>> operatorGraph = new HashMap<>();

  private final MessageStreamGraphImpl streamGraph;

  public StreamOperatorAdaptorTask(MessageStreamGraphImpl graph) {
    this.streamGraph = graph;
  }

  @Override
  public final void init(Config config, TaskContext context) throws Exception {
    // TODO: does it work if multiple SSPs are buffered by the same operatorImpl object?????
    // Alternative: insert a merge stage that add one merge operator per SSP
    Map<SystemStream, MessageStreamImpl> inputBySystemStream = new HashMap<>();
    context.getSystemStreamPartitions().forEach(ssp -> {
        if (!inputBySystemStream.containsKey(ssp.getSystemStream())) {
          inputBySystemStream.putIfAbsent(ssp.getSystemStream(), this.streamGraph.getStreamByName(ssp.getSystemStream()));
        }
      });
    context.getSystemStreamPartitions().forEach(ssp -> operatorGraph.put(ssp,
        OperatorImpls.createOperatorImpls(inputBySystemStream.get(ssp.getSystemStream()), context)));
  }

  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    this.operatorGraph.get(ime.getSystemStreamPartition().getSystemStream())
        .onNext(new IncomingSystemMessageEnvelope(ime), collector, coordinator);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO: invoke timer based triggers
  }
}
