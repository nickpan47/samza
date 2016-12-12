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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemStream;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBasePipeline {
  Processor repartition;
  Processor main;
  List<Processor> processors;

  PStream inputStream1;
  PStream inputStream2;
  PStream intermediateStream1;
  PStream intermediateStream2;
  PStream outputStream1;
  List<PStream> streams;

  @Before
  public void setUp() {
    repartition = new Processor("Repartitioner");
    main = new Processor("MyProcessor");
    processors = Lists.newArrayList(repartition, main);

    inputStream1 = createStream("InputStream1", PStream.Visibility.PUBLIC);
    inputStream2 = createStream("InputStream2", PStream.Visibility.PUBLIC);
    intermediateStream1 = createStream("IntermediateStream1", PStream.Visibility.PRIVATE);
    intermediateStream2 = createStream("IntermediateStream2", PStream.Visibility.PUBLIC);
    outputStream1 = createStream("OutputStream", PStream.Visibility.PUBLIC);
    streams = Lists.newArrayList(inputStream1, inputStream2, intermediateStream1, intermediateStream2, outputStream1);
  }

  @Test
  public void testBasicPipeline() {
    PipelineBuilder builder = new PipelineBuilder();
    Pipeline pipeline = builder.addInputStreams(repartition, inputStream1, inputStream2)
                                .addIntermediateStreams(repartition, main, intermediateStream1, intermediateStream2)
                                .addOutputStreams(main, outputStream1)
                                .build();

    assertEquals(processors.size(), pipeline.getAllProcessors().size());
    assertTrue(pipeline.getAllProcessors().containsAll(processors));

    assertEquals(streams.size(), pipeline.getAllStreams().size());
    assertTrue(pipeline.getAllStreams().containsAll(streams));

    List<PStream> inputs = Lists.newArrayList(inputStream1, inputStream2);
    assertEquals(inputs.size(), pipeline.getSourceStreams().size());
    assertTrue(pipeline.getSourceStreams().containsAll(inputs));

    List<PStream> intermediates = Lists.newArrayList(intermediateStream1, intermediateStream2);
    assertEquals(intermediates.size(), pipeline.getIntermediateStreams().size());
    assertTrue(pipeline.getIntermediateStreams().containsAll(intermediates));

    List<PStream> outputs = Lists.newArrayList(outputStream1);
    assertEquals(outputs.size(), pipeline.getSinkStreams().size());
    assertTrue(pipeline.getSinkStreams().containsAll(outputs));

    assertEquals(4, pipeline.getPublicStreams().size());
    assertTrue(pipeline.getPublicStreams().containsAll(ImmutableList.of(inputStream1, inputStream2, intermediateStream2, outputStream1)));

    assertEquals(1, pipeline.getPrivateStreams().size());
    assertTrue(pipeline.getPrivateStreams().contains(intermediateStream1));
  }

  /**
   * We should not allow empty Pipelines to be built because Pipelines are immutable and an empty
   * Pipeline wouldn't do anything.
   */
  @Test(expected = IllegalStateException.class)
  public void testValidationEmptyPipeline() {
    new PipelineBuilder().build();
  }

  // #### Source streams ####
  /**
   * We should not allow Pipelines to be built with no source streams. They would not process anything (assumption).
   */
  @Test(expected = IllegalStateException.class)
  public void testValidationNoSourceStreams() {
    new PipelineBuilder().addProcessor(repartition).build();
  }

  /**
   * Cannot add null streams
   */
  @Test(expected = NullPointerException.class)
  public void testValidationNullSourceStream() {
    new PipelineBuilder().addInputStreams(repartition, null).build();
  }

  /**
   * Cannot add null streams
   */
  @Test(expected = NullPointerException.class)
  public void testValidationNullSourceStreamSecondPosition() {
    new PipelineBuilder().addInputStreams(repartition, new PStream[] {inputStream1, null}).build();
  }

  /**
   * Cannot add streams to a null processor
   */
  @Test(expected = NullPointerException.class)
  public void testValidationSourceStreamWithNullProcessor() {
    new PipelineBuilder().addInputStreams(null, inputStream1).build();
  }

  /**
   * Streams added with no producer should be SOURCE type.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidSourceStreamTypeIntermediate() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addInputStreams(repartition, intermediateStream2).build();
  }

  /**
   * Streams added with no producer should be SOURCE type.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidSourceStreamTypeSink() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addInputStreams(repartition, outputStream1).build();
  }

  /**
   * Source streams should always have public visibility.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidSourceStreamVisibility() {
    new PipelineBuilder().addInputStreams(repartition, createStream("InputStream3", PStream.Visibility.PRIVATE)).build();
  }

  /**
   * Source streams should have at least 1 consumer.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationUnusedSourceStream() {
    new BasePipeline("pName", Collections.singletonList(repartition), Collections.singletonList(inputStream1), Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Source streams should not have any producers.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationSourceStreamWithProducer() {
    new BasePipeline("pName", Collections.singletonList(repartition),
                    Collections.singletonList(inputStream1),
                    Collections.singletonMap(inputStream1,
                        Collections.singletonList(repartition)),
                    Collections.emptyMap());
  }

  // #### Intermediate streams ####
  /**
   * We should allow Pipelines to be built with no intermediate streams.
   * All processors would consume 1:n source streams and produce to 0:n sink streams.
   */
  @Test
  public void testValidationNoIntermediateStreams() {
    Pipeline pipeline = new PipelineBuilder().addInputStreams(repartition, inputStream1)
                                              .addInputStreams(main, inputStream2)
                                              .addOutputStreams(repartition, outputStream1).build();

    assertEquals(2, pipeline.getAllProcessors().size());
    assertTrue(pipeline.getAllProcessors().containsAll(ImmutableList.of(repartition, main)));

    assertEquals(3, pipeline.getAllStreams().size());
    assertTrue(pipeline.getAllStreams().containsAll(ImmutableList.of(inputStream1, inputStream2, outputStream1)));

    assertEquals(3, pipeline.getPublicStreams().size());
    assertTrue(pipeline.getPublicStreams().containsAll(ImmutableList.of(inputStream1, inputStream2, outputStream1)));

    assertEquals(2, pipeline.getSourceStreams().size());
    assertTrue(pipeline.getSourceStreams().containsAll(ImmutableList.of(inputStream1, inputStream2)));

    assertEquals(1, pipeline.getSinkStreams().size());
    assertTrue(pipeline.getSinkStreams().containsAll(ImmutableList.of(outputStream1)));

    assertEquals(0, pipeline.getIntermediateStreams().size());
    assertEquals(0, pipeline.getPrivateStreams().size());
  }

  /**
   * Cannot add null streams
   */
  @Test(expected = NullPointerException.class)
  public void testValidationNullIntermediateStream() {
    new PipelineBuilder().addIntermediateStreams(repartition, main, null).build();
  }

  /**
   * Cannot add null streams
   */
  @Test(expected = NullPointerException.class)
  public void testValidationNullIntermediateStreamSecondPosition() {
    new PipelineBuilder().addIntermediateStreams(repartition, main, new PStream[] {intermediateStream1, null}).build();
  }

  /**
   * Cannot add streams to a null processor
   */
  @Test(expected = NullPointerException.class)
  public void testValidationIntermediateStreamWithNullUpstreamProcessor() {
    new PipelineBuilder().addIntermediateStreams(null, main, intermediateStream1).build();
  }

  /**
   * Cannot add streams to a null processor
   */
  @Test(expected = NullPointerException.class)
  public void testValidationIntermediateStreamWithNullDownstreamProcessor() {
    new PipelineBuilder().addIntermediateStreams(repartition, null, intermediateStream1).build();
  }

  /**
   * Cannot add streams to a null processor
   */
  @Test(expected = NullPointerException.class)
  public void testValidationIntermediateStreamWithNullProcessors() {
    new PipelineBuilder().addIntermediateStreams(null, null, intermediateStream1).build();
  }

  /**
   * Streams added with both a producer and consumer should be INTERMEDIATE type.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidIntermediateStreamTypeSource() {
    new PipelineBuilder().addIntermediateStreams(repartition, main, inputStream1).build();
  }

  /**
   * Streams added with both a producer and consumer should be INTERMEDIATE type.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidIntermediateStreamTypeSink() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addIntermediateStreams(repartition, main, outputStream1).build();
  }

  /**
   * Intermediate streams should be allowed to be PRIVATE visibility.
   */
  @Test
  public void testValidationIntermediateStreamVisibilityPrivate() {
    PStream privateStream = createStream("IntermediateStream3", PStream.Visibility.PRIVATE);
    Pipeline pipeline = new PipelineBuilder().addIntermediateStreams(repartition, main, privateStream)
        .addInputStreams(repartition, inputStream1).build();

    assertEquals(2, pipeline.getAllProcessors().size());
    assertTrue(pipeline.getAllProcessors().containsAll(ImmutableList.of(repartition, main)));

    assertEquals(2, pipeline.getAllStreams().size());
    assertTrue(pipeline.getAllStreams().contains(privateStream));

    assertEquals(1, pipeline.getSourceStreams().size());
    assertTrue(pipeline.getSourceStreams().contains(inputStream1));

    assertEquals(1, pipeline.getIntermediateStreams().size());
    assertTrue(pipeline.getIntermediateStreams().contains(privateStream));

    assertEquals(1, pipeline.getPrivateStreams().size());
    assertTrue(pipeline.getPrivateStreams().contains(privateStream));

    assertEquals(1, pipeline.getPublicStreams().size());
    assertTrue(pipeline.getPublicStreams().contains(inputStream1));

    assertEquals(0, pipeline.getSinkStreams().size());
  }

  /**
   * Intermediate streams should have at least 1 producer and consumer.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationUnusedIntermediateStream() {
    new BasePipeline("pName", Collections.singletonList(repartition),
        ImmutableList.of(inputStream1, intermediateStream1),
        Collections.emptyMap(),
        Collections.singletonMap(inputStream1, Collections.singletonList(repartition))); // Avoid error for no source streams
  }

  /**
   * Intermediate streams should have at least 1 producer and consumer.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationIntermediateStreamWithNoProducer() {
    new BasePipeline("pName", Collections.singletonList(repartition),
        ImmutableList.of(inputStream1, intermediateStream1),
        Collections.emptyMap(),
        ImmutableMap.of(inputStream1, Collections.singletonList(repartition), // Avoid error for no source streams
            intermediateStream1, Collections.singletonList(repartition)));
  }

  /**
   * Intermediate streams should have at least 1 producer and consumer.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationIntermediateStreamWithNoConsumer() {
    new BasePipeline("pName", Collections.singletonList(repartition),
        ImmutableList.of(inputStream1, intermediateStream1),
        Collections.singletonMap(intermediateStream1, Collections.singletonList(repartition)),
        Collections.singletonMap(inputStream1, Collections.singletonList(repartition))); // Avoid error for no source streams
  }

  // #### Sink streams ####

  /**
   * We should allow Pipelines to be built with no sink streams. The ultimate goal may be to write to a DB, for example.
   */
  @Test
  public void testValidationNoSinkStreams() {
    Pipeline pipeline = new PipelineBuilder().addInputStreams(repartition, inputStream1).build();

    assertEquals(1, pipeline.getAllProcessors().size());
    assertTrue(pipeline.getAllProcessors().contains(repartition));

    assertEquals(1, pipeline.getAllStreams().size());
    assertTrue(pipeline.getAllStreams().contains(inputStream1));

    assertEquals(1, pipeline.getSourceStreams().size());
    assertTrue(pipeline.getSourceStreams().contains(inputStream1));
  }

  /**
   * Cannot add null streams
   */
  @Test(expected = NullPointerException.class)
  public void testValidationNullSinkStream() {
    new PipelineBuilder().addOutputStreams(repartition, null).build();
  }

  /**
   * Cannot add null streams
   */
  @Test(expected = NullPointerException.class)
  public void testValidationNullSinkStreamSecondPosition() {
    new PipelineBuilder().addOutputStreams(repartition, new PStream[] {outputStream1, null}).build();
  }

  /**
   * Cannot add streams to a null processor
   */
  @Test(expected = NullPointerException.class)
  public void testValidationSinkStreamWithNullProcessor() {
    new PipelineBuilder().addOutputStreams(null, outputStream1).build();
  }

  /**
   * Streams added with no consumer should be SINK type.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidSinkStreamTypeIntermediate() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addOutputStreams(repartition, intermediateStream2).build();
  }

  /**
   * Streams added with no consumer should be SINK type.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidSinkStreamTypeSource() {
    new PipelineBuilder().addOutputStreams(repartition, inputStream1).build();
  }

  /**
   * Sink streams should always have public visibility.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationInvalidSinkStreamVisibility() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addOutputStreams(repartition, createStream("OutputStream2", PStream.Visibility.PRIVATE)).build();
  }

  /**
   * Sink streams should have at least 1 producer.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationUnusedSinkStream() {
    new BasePipeline("pName", Collections.singletonList(repartition),
                      ImmutableList.of(inputStream1, outputStream1),
                      Collections.emptyMap(),  // No producer for the sink stream!
                      Collections.singletonMap(inputStream1, Collections.singletonList(repartition))); // Avoid error for no source streams
  }

  /**
   * Sink streams should not have any consumers.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testValidationSinkStreamWithConsumer() {
    new BasePipeline("pName", Collections.singletonList(repartition),
        ImmutableList.of(inputStream1, outputStream1),
        Collections.emptyMap(),
        ImmutableMap.of(inputStream1, Collections.singletonList(repartition), // Avoid error for no source streams
        outputStream1, Collections.singletonList(repartition))); // Sink stream with a consumer!
  }

  // #### Cycles ####
  @Test(expected = IllegalStateException.class)
  public void testProcessorCycleSelf() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addIntermediateStreams(repartition, main, intermediateStream1)
        .addIntermediateStreams(main, main, intermediateStream2) // CYCLE!
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testProcessorCycleTwoHops() {
    new PipelineBuilder().addInputStreams(repartition, inputStream1) // Avoid error for no source streams
        .addIntermediateStreams(repartition, main, intermediateStream1)
        .addIntermediateStreams(main, repartition, intermediateStream2) // CYCLE!
        .build();
  }

  // MISC
//  @Test(expected = IllegalArgumentException.class)
//  public void testInvalidProducerMapStreamEmpty() {
//    new BasePipeline(Collections.singletonList(repartition),
//        Collections.singletonList(inputStream1),
//        Collections.singletonMap(inputStream1, Collections.emptyList()), // Map to empty!
//        Collections.singletonMap(inputStream1, Collections.singletonList(repartition))); // Avoid error for no source streams
//
//    // TODO check assertions before deciding if an error should be thrown
//  }
//
//  @Test(expected = IllegalArgumentException.class)
//  public void testInvalidConsumerMapStreamEmpty() {
//    new BasePipeline(Collections.singletonList(repartition),
//        Collections.singletonList(inputStream1),
//        Collections.emptyMap(),
//        Collections.singletonMap(inputStream1, Collections.emptyList())); // Map to empty!
//    // TODO check assertions before deciding if an error should be thrown
//  }

  private PStream createStream(String name, PStream.Visibility visibility) {
    return new PStream(name, new SystemStream("kafka", name), 4, visibility);
  }
}
