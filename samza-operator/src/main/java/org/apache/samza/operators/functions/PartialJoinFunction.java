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
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.task.TaskContext;


/**
 * This defines the interface function a two-way join functions that takes input messages from two input
 * {@link org.apache.samza.operators.MessageStream}s and merge them into a single output joined message in the join output
 */
@InterfaceStability.Unstable
public interface PartialJoinFunction<K, M, OM, RM> extends InitFunction{

  /**
   * Method to perform join method on the two input messages
   *
   * @param m1  message from the first input stream
   * @param om  message from the second input stream
   * @return  the joined message in the output stream
   */
  RM apply(M m1, OM om);

  /**
   * Method to get the key from the input message
   *
   * @param message  the input message from the first {@link MessageStream}
   * @return  the join key in the {@code message}
   */
  K getKey(M message);

  /**
   * Method to get the key from the input message in the other {@link MessageStream}
   *
   * @param message  the input message from the other {@link MessageStream}
   * @return  the join key in the {@code message}
   */
  K getOtherKey(OM message);

  /**
   * Init method to initialize the context for this {@link PartialJoinFunction}. The default implementation is NO-OP.
   *
   * @param config  the {@link Config} object for this task
   * @param context  the {@link TaskContext} object for this task
   */
  default void init(Config config, TaskContext context) { }
}
