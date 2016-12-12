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

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.api.PipelineFactory;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemStream;
import scala.collection.JavaConversions;

/**
 * A PipelineFactory that handles the migration scenario by wrapping a job
 * as a single-stage Pipeline.
 */
public class SingleStagePipelineFactory implements PipelineFactory {
  @Override
  public Pipeline create(Config config) {
    // TODO the config structure will be legacy. Job configs will not be in some nested namespace, so translate if needed.
    Processor processor = getProcessor(config);
    PStream[] streams = getInputs(config);

    PipelineBuilder builder = new PipelineBuilder(processor.getName());
    return builder.addInputStreams(processor, streams).build();
  }

  private Processor getProcessor(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    return new Processor(jobConfig.getName().get()); // // TODO: 10/26/16 at LI, job name doesn't exist until after rewrite, I think
  }

  private PStream[] getInputs(Config config) {
    TaskConfig taskConfig = new TaskConfig(config);
    List<SystemStream> systemStreams = new ArrayList(JavaConversions.asJavaSet(taskConfig.getInputStreams()));
    PStream[] streams = new PStream[systemStreams.size()];
    for (int i = 0; i < systemStreams.size(); i++) {
      SystemStream ss = systemStreams.get(i);
      streams[i] = new PStream(ss.getStream(), ss, -1);  // // TODO: 10/26/16 defaults for partition and key extractor not great
    }
    return streams;
  }
}
