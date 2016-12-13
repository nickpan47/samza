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

import org.apache.samza.operators.MessageStreamsBuilder;
import org.apache.samza.operators.MessageStreamsBuilderImpl;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemStream;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class TestBasePipelineWithStreamsBuilder {
  private StreamOperatorProcessor main;

  private PStream inputStream1;
  private PStream inputStream2;
  private PStream intermediateStream1;
  private PStream intermediateStream2;
  private PStream outputStream1;

  @Before
  public void setup() {
    this.inputStream1 = this.createStream("kafka.InputStream1", PStream.Visibility.PUBLIC);
    this.inputStream2 = this.createStream("kafka.InputStream2", PStream.Visibility.PUBLIC);
    this.intermediateStream1 = this.createStream("kafka.mytopic-1", PStream.Visibility.PUBLIC);
    this.intermediateStream2 = this.createStream("kafka.mytopic-2", PStream.Visibility.PUBLIC);
    this.outputStream1 = this.createStream("kafka.output-1", PStream.Visibility.PUBLIC);

    // This describes a DAG w/ multiple stages
    MessageStreamsBuilder mstreamsBuilder = new MessageStreamsBuilderImpl();
    mstreamsBuilder.addInputStream(new SystemStream("kafka", "InputStream1")).through(
        new SystemStream("kafka", "mytopic-1")).
        join(mstreamsBuilder.addInputStream(new SystemStream("kafka", "InputStream2")).through(
                new SystemStream("kafka", "mytopic-2")),
            (m1, m2) -> new JsonIncomingSystemMessageEnvelope<>(m1.getKey().toString(),
                new HashMap<Object, Object>() {
                  {
                    this.putAll((Map<Object, Object>) m1.getMessage());
                    this.putAll((Map<Object, Object>) m2.getMessage());
                  }
                }, null, null)).
        window(Windows.intoSessionCounter(m -> String.format("%s", m.getMessage().get("treeId")))).
        sink(new SystemStream("kafka", "output-1"));

    main = new StreamOperatorProcessor("MyProcessor", mstreamsBuilder);
  }

  @Test public void TestBasePipeline() {
    PipelineBuilder builder = new PipelineBuilder();

    // this creates an execution plan that only have one job
    Pipeline pipeline = builder.addInputStreams(main, inputStream1, inputStream2, intermediateStream1, intermediateStream2)
        .addOutputStreams(main, outputStream1).build();
  }

  private PStream createStream(String name, PStream.Visibility visibility) {
    return new PStream(name, new SystemStream("kafka", name), 4, visibility);
  }

}
