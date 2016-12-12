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

package org.apache.samza.pipeline.api;

import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.pipeline.Processor;
import org.apache.samza.pipeline.stream.PStream;


// TODO: This interface may not be needed. Depends on whether there is any contract we want to enforce.
public interface Pipeline {
  List<Processor> getAllProcessors();
  List<Processor> getProcessorsInStartOrder();

  List<PStream> getAllStreams();
  List<PStream> getManagedStreams();
  List<PStream> getPrivateStreams();
  List<PStream> getPublicStreams();
  List<PStream> getSourceStreams();
  List<PStream> getIntermediateStreams();
  List<PStream> getSinkStreams();

  /**
   *
   * @param stream
   * @return the list of producers in this pipeline for the specified stream. Never null.
   */
  List<Processor> getStreamProducers(PStream stream);

  /**
   *
   * @param stream
   * @return the list of consumers in this pipeline for the specified stream. Never null.
   */
  List<Processor> getStreamConsumers(PStream stream);
  List<PStream> getProcessorOutputs(Processor proc);
  List<PStream> getProcessorInputs(Processor proc);

  Config getProcessorConfig(Processor proc);
  Config getStreamConfig(PStream stream);
}
