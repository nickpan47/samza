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
package org.apache.samza.operators.spec;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.task.TaskContext;


/**
 * A stateless serializable stream operator specification that holds all the information required
 * to transform the input {@link MessageStreamImpl} and produce the output {@link MessageStreamImpl}.
 */
@InterfaceStability.Unstable
public interface OperatorSpec<OM extends MessageEnvelope> {

  enum OpCode {
    MAP,
    FLAT_MAP,
    FILTER,
    SINK,
    SEND_TO,
    JOIN,
    WINDOW,
    MERGE,
    KEYED_BY
  }


  /**
   * Get the output stream containing transformed {@link MessageEnvelope} produced by this operator.
   * @return  the output stream containing transformed {@link MessageEnvelope} produced by this operator.
   */
  MessageStreamImpl<OM> getOutputStream();

  default void init(Config config, TaskContext taskContext) { }
}
