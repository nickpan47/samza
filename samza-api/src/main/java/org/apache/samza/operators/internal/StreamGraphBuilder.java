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
package org.apache.samza.operators.internal;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.operators.StreamGraph;


/**
 * Internally used interface class to create an empty {@link StreamGraph} object.
 */
@InterfaceStability.Unstable
public interface StreamGraphBuilder {
  String BUILDER_CONFIG = "job.stream.graph.builder";
  String DEFAULT_BUILDER_CLASS = "org.apache.samza.operators.internal.StreamGraphBuilderImpl";

  /**
   * Interface method to create an empty {@link StreamGraph} object
   *
   * @return  an empty {@link StreamGraph} object
   */
  StreamGraph create();

  /**
   * Static method to load the {@link StreamGraphBuilder} class. By default, it always loads internal implementation class.
   *
   * @param config  configuration object for this task
   * @return  the {@link StreamGraphBuilder} object
   */
  static StreamGraphBuilder fromConfig(Config config) {
    try {
      if (StreamGraphBuilder.class.isAssignableFrom(Class.forName(config.get(BUILDER_CONFIG, DEFAULT_BUILDER_CLASS)))) {
        return (StreamGraphBuilder) Class.forName(config.get(BUILDER_CONFIG, DEFAULT_BUILDER_CLASS)).newInstance();
      }
    } catch (Exception e) {
      throw new ConfigException(String.format("Problem in loading StreamGraphBuilder class %s", config.get(BUILDER_CONFIG)), e);
    }
    throw new ConfigException(String.format(
        "StreamGraphBuilder class %s does not implement interface StreamGraphBuilder properly",
        config.get(BUILDER_CONFIG)));
  }
}
