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
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.JobRunner;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.ExtendedSystemAdmin;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.Systems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PipelineRunner {
  private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);
  public static final String CONFIG_STREAM_PREFIX = "systems.%s.streams.%s."; // TODO do these belong in a PipelineConfig class?
  public static final String CONFIG_PROCESSOR_PREFIX = "processors.%s.";
  private static final boolean INHERIT_ROOT_CONFIGS = true; // Temp switch until we decide whether we want this behavior.
  private final Pipeline pipeline;
  private final Config config;

  public PipelineRunner(Pipeline pipeline, Config config) {
    this.pipeline = pipeline;
    this.config = config;
  }

  public void run() {
    log.info("Got config {}", config);
    log.info("Got pipeline {}", pipeline);

    createStreams(pipeline, config);

    for (Processor processor : pipeline.getProcessorsInStartOrder()) {
      startProcessor(pipeline, processor);
    }
    // todo exceptions if any issue. Perhaps even roll back if we can't tolerate partial deployments.
  }

  public void kill() {
    log.info("Got config {}", config);
    log.info("Got pipeline {}", pipeline);

    for (Processor processor : pipeline.getProcessorsInStartOrder()) {
      killProcessor(pipeline, processor);
    }
  }

  public ApplicationStatus status() {
    log.info("Got config {}", config);
    log.info("Got pipeline {}", pipeline);

    if (pipeline.getAllProcessors().isEmpty()) {
      throw new SamzaException("Pipeline has no processors. Status is undefined.");
    }

    for (Processor processor : pipeline.getProcessorsInStartOrder()) {
      ApplicationStatus status = getStatus(pipeline, processor);
      log.info("Got status {} for processor {}", status, processor);
      if (!(status == ApplicationStatus.New || status == ApplicationStatus.Running)) {
        return status;
      }
    }
    return ApplicationStatus.Running;
  }

  private void createStreams(Pipeline pipeline, Config config) {
    // Build system -> streams map
    Multimap<String, PStream> streamsGroupedBySystem = HashMultimap.create();
    pipeline.getManagedStreams().forEach(pStream -> streamsGroupedBySystem.put(pStream.getSystem(), pStream));

    for (Map.Entry<String, Collection<PStream>> entry : streamsGroupedBySystem.asMap().entrySet()) {
      String systemName = entry.getKey();
      SystemAdmin systemAdmin = Systems.getSystemAdmins(config).get(systemName);

      log.info("Creating streams on system {}", systemName);
      for (PStream stream : entry.getValue()) {
        createStream(config, (ExtendedSystemAdmin) systemAdmin, stream);
      }
    }
  }

  private void createStream(Config config, ExtendedSystemAdmin systemAdmin, PStream stream) {
    Config streamConfig = extractStreamConfig(config, stream);

    log.info("Creating stream {} with config {}", stream, streamConfig);
    systemAdmin.createStream(stream.getSystemStream(), config);
  }

  private void startProcessor(Pipeline pipeline, Processor processor) {
    Config procConfig = extractProcessorConfig(config, processor, pipeline);

    log.info("Starting processor {} with config {}", processor, procConfig);
    JobRunner runner = new JobRunner(JobRunner.rewriteConfig(procConfig));
    runner.run(true);
  }

  private void killProcessor(Pipeline pipeline, Processor processor) {
    Config procConfig = extractProcessorConfig(config, processor, pipeline);

    log.info("Killing processor {} with config {}", processor, procConfig);
    JobRunner runner = new JobRunner(JobRunner.rewriteConfig(procConfig));
    runner.kill();
  }

  private ApplicationStatus getStatus(Pipeline pipeline, Processor processor) {
    Config procConfig = extractProcessorConfig(config, processor, pipeline);

    log.info("Getting status for processor {} with config {}", processor, procConfig);
    JobRunner runner = new JobRunner(JobRunner.rewriteConfig(procConfig));
    return runner.status();
  }

  private Config extractProcessorConfig(Config config, Processor processor, Pipeline pipeline) {
    String configPrefix = String.format(CONFIG_PROCESSOR_PREFIX, processor.getName());
    // TODO: Disallow user specifying processor inputs/outputs. This info comes strictly from the pipeline.
    return extractScopedConfig(config, pipeline.getProcessorConfig(processor), configPrefix);
  }

  private Config extractStreamConfig(Config config, PStream stream) {
    String configPrefix = String.format(CONFIG_STREAM_PREFIX, stream.getSystem(), stream.getStream());
    return extractScopedConfig(config, pipeline.getStreamConfig(stream), configPrefix);
  }

  private Config extractScopedConfig(Config fullConfig, Config generatedConfig, String configPrefix) {
    Config scopedConfig = fullConfig.subset(configPrefix);
    log.info("Prefix '{}' has extracted config {}", configPrefix, scopedConfig);
    log.info("Prefix '{}' has generated config {}", configPrefix, generatedConfig);

    Config[] configPrecedence;
    if (INHERIT_ROOT_CONFIGS) {
      configPrecedence = new Config[] {fullConfig, generatedConfig, scopedConfig};
    } else {
      configPrecedence = new Config[] {generatedConfig, scopedConfig};
    }

    // TODO: Refactor
    // Strip empty configs so they don't override the configs before them.
    Map<String, String> mergedConfig = new HashMap<>();
    for (Map<String, String> config : configPrecedence) {
      for (Map.Entry<String, String> property : config.entrySet()) {
        String value = property.getValue();
        if (!(value == null || value.isEmpty())) {
          mergedConfig.put(property.getKey(), property.getValue());
        }
      }
    }
    scopedConfig = new MapConfig(mergedConfig);
//    scopedConfig = new MapConfig(mergedConfigArrays.asList(configPrecedence));

    log.info("Prefix '{}' has merged config {}", configPrefix, scopedConfig);

    return scopedConfig;
  }
}