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

import org.apache.samza.system.IncomingMessageEnvelope;


/**
 * Represents empirically and semantically how streams are keyed.
 *
 * When you talk about streams, you say things like "they're both keyed on member ID"
 * That is a semantic concept like a foreign key that can be independent of message/key structure. But unfortunately it is mostly taken on faith.
 *
 * However, you still need to know procedurally how to extract the key, since the messages/keys could have different
 * structures.
 *
 * Implementations of this interface should address both of these items.
 */
public interface KeyExtractor {
  // An identifier that indicates how the partitions are keyed. Taken on faith?
  String getPartitionKeyName();

  Object extractKey(IncomingMessageEnvelope message); // TODO IncomingMessageEnvelope or other type?
}
