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

package org.apache.samza.pipeline;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;

import java.util.HashSet;
import java.util.Set;


public class PipelineBuilder {
  private final MutablePipeline pipeline = new MutablePipeline();

  public PipelineBuilder(String pipelineName) {
    pipeline.name = pipelineName;
  }

  public PipelineBuilder() {
    // Intentionally left blank
  }

  /**
   * Builds the topology.
   *
   * @return an instance of {@link Pipeline}
   */
  public Pipeline build() {
    return new BasePipeline(pipeline.name,
                            Lists.newArrayList(pipeline.processors),
                            Lists.newArrayList(pipeline.streams),
                            pipeline.streamProducers.asMap(),
                            pipeline.streamConsumers.asMap());
  }

  public PipelineBuilder setPipelineName(String pipelineName) {
    pipeline.name = pipelineName;
    return this;
  }

  /**
   * Adds a processor, initially with no streams.
   *
   * TODO
   * @param processor the processor to add to the topology.
   * @return          this builder, for chaining.
   */
  public PipelineBuilder addProcessor(Processor processor) {
    pipeline.processors.add(processor);
    return this;
  }

  /**
   * Adds a processor (if necessary) and associates a list of streams as inputs.
   *
   * @param processor
   * @param streams
   * @return
   */
  // TODO: Alternate name could be addHeadProcessor if we want users to focus on processors rather than streams.
  public PipelineBuilder addInputStreams(Processor processor, PStream... streams) {
    addProcessor(processor);
    for (PStream stream: streams) {
      addStream(stream);
      pipeline.streamConsumers.put(stream, processor);
    }
    return this;
  }

  /**
   * Adds two processors (if necessary) and connects them with the list of streams.
   *
   * @param producer
   * @param consumer
   * @param streams
   * @return
   */
  // TODO Alternate name could be addDownstreamProcessor(producer, consumer, streams...) if we want users to focus on processors rather than streams.
  // TODO it's confusing to refer to input, intermediate and output streams when the pipeline terminology is source, int, sink
  // TODO maybe it should be:
  // addConsumer() or addProcessorWithInputs()
  // addProducerConsumer()
  // addProducer() or addProcessorWithOutputs()
  public PipelineBuilder addIntermediateStreams(Processor producer, Processor consumer, PStream... streams) {
    addProcessor(producer);
    addProcessor(consumer);
    for (PStream stream: streams) {
      addStream(stream);
      pipeline.streamProducers.put(stream, producer);
      pipeline.streamConsumers.put(stream, consumer);
    }
    return this;
  }

  /**
   * Adds a processor (if necessary) and associates a list of streams as outputs.
   *
   * TODO if we had a stream template that knew the system, partition count and key
   * @param processor
   * @param streams
   * @return
   */
  // TODO: Alternate name could be addTailProcessor if we want users to focus on processors rather than streams.
  public PipelineBuilder addOutputStreams(Processor processor, PStream... streams) {
    addProcessor(processor);
    for (PStream stream : streams) {
      addStream(stream);
      pipeline.streamProducers.put(stream, processor);
    }
    return this;
  }

  private PipelineBuilder addStream(PStream stream) {
    pipeline.streams.add(stream);
    return this;
  }

  private static class MutablePipeline {
    private String name;
    private final Set<Processor> processors = new HashSet<>(); // should be initialized with an unmodifiable list
    private final Set<PStream> streams = new HashSet<>(); // should be initialized with an unmodifiable list

    private final Multimap<PStream, Processor> streamProducers = HashMultimap.create(); // Map from stream name to its producers
    private final Multimap<PStream, Processor> streamConsumers = HashMultimap.create(); // Map from stream name to its consumers
  }
}
