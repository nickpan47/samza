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
package org.apache.samza.operators.functions;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;


/**
 * A function that joins {@link org.apache.samza.operators.data.MessageEnvelope}s from two {@link org.apache.samza.operators.MessageStream}s and produces
 * a joined {@link org.apache.samza.operators.data.MessageEnvelope}.
 * @param <M>  type of the input {@link org.apache.samza.operators.data.MessageEnvelope}
 * @param <JM>  type of the {@link org.apache.samza.operators.data.MessageEnvelope} to join with
 * @param <RM>  type of the joined {@link org.apache.samza.operators.data.MessageEnvelope}
 */
@InterfaceStability.Unstable
public interface JoinFunction<M extends MessageEnvelope, JM extends MessageEnvelope, RM extends MessageEnvelope> extends ContextInitFunction {

  /**
   * Join the provided {@link org.apache.samza.operators.data.MessageEnvelope}s and produces the joined {@link org.apache.samza.operators.data.MessageEnvelope}.
   * @param message  the input {@link org.apache.samza.operators.data.MessageEnvelope}
   * @param otherMessage  the {@link org.apache.samza.operators.data.MessageEnvelope} to join with
   * @return  the joined {@link org.apache.samza.operators.data.MessageEnvelope}
   */
  RM apply(M message, JM otherMessage);

}
