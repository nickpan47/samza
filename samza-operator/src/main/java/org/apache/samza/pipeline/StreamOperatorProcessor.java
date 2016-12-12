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


public class StreamOperatorProcessor extends Processor {

  private final MessageStreamsBuilder streamsBuilder;

  public StreamOperatorProcessor(String name, MessageStreamsBuilder streamsBuilder) {
    super(name);
    this.streamsBuilder = streamsBuilder;
  }

  // TODO: need to work on a serialized format of {@link MessageStreamsBuilder} object s.t. the definition of {@link StreamOperatorProcessor}
  // can be serialized to config and passed around between {@link JobRunner}, {@link JobCoordinator}, and {@link SamzaContainer}
}
