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

import com.google.common.collect.Lists;
import org.apache.samza.operators.MessageStreamsBuilder;
import org.apache.samza.operators.MessageStreamsBuilderImpl;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemStream;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestBasePipelineWithStreamsBuilder {
  private Processor repartition;
  private StreamOperatorProcessor main;
  private List<Processor> processors;

  private PStream inputStream1;
  private PStream inputStream2;
  private PStream intermediateStream1;
  private PStream intermediateStream2;
  private PStream outputStream1;
  private List<PStream> streams;

  @Before
  public void setup() {

    MessageStreamsBuilder repartitionBuilder = new MessageStreamsBuilderImpl();
    repartitionBuilder.addInputStream(new SystemStream("kafka", "InputStream1")).sink(new SystemStream("kafka", "mytopic-1"));
    repartitionBuilder.addInputStream(new SystemStream("kafka", "InputStream2")).sink(new SystemStream("kafka", "mytopic-2"));
    repartition = new StreamOperatorProcessor("Repartitioner", repartitionBuilder);

    MessageStreamsBuilder streamsBuilder = new MessageStreamsBuilderImpl();
    streamsBuilder.<IncomingSystemMessageEnvelope>addInputStream(new SystemStream("kafka", "mytopic-1")).
        join(streamsBuilder.<IncomingSystemMessageEnvelope>addInputStream(new SystemStream("kafka", "mytopic-2")),
            (m1, m2) -> new JsonIncomingSystemMessageEnvelope<>(m1.getKey().toString(),
                new HashMap<Object, Object>() {
                  {
                    this.putAll((Map<Object, Object>) m1.getMessage());
                    this.putAll((Map<Object, Object>) m2.getMessage());
                  }
                }, null, null)).
        window(Windows.intoSessionCounter(m -> String.format("%s", m.getMessage().get("treeId")))).
        sink(new SystemStream("kafka", "output-1"));

    main = new StreamOperatorProcessor("MyProcessor", streamsBuilder);
    processors = Lists.newArrayList(repartition, main);

    inputStream1 = createStream("InputStream1", PStream.Visibility.PUBLIC);
    inputStream2 = createStream("InputStream2", PStream.Visibility.PUBLIC);
    intermediateStream1 = createStream("mytopic-1", PStream.Visibility.PRIVATE);
    intermediateStream2 = createStream("mytopic-2", PStream.Visibility.PRIVATE);
    outputStream1 = createStream("output-1", PStream.Visibility.PUBLIC);
    streams = Lists.newArrayList(inputStream1, inputStream2, intermediateStream1, intermediateStream2, outputStream1);
  }

  @Test public void TestBasePipeline() {
    PipelineBuilder builder = new PipelineBuilder();

    Pipeline pipeline = builder.addInputStreams(repartition, inputStream1, inputStream2)
        .addIntermediateStreams(repartition, main, intermediateStream1, intermediateStream2)
        .addOutputStreams(main, outputStream1).build();
  }

  private PStream createStream(String name, PStream.Visibility visibility) {
    return new PStream(name, new SystemStream("kafka", name), 4, visibility);
  }

}
