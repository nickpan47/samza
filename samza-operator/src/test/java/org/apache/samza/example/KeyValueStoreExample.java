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
package org.apache.samza.example;

import com.sun.org.glassfish.external.statistics.Stats;
import java.util.concurrent.TimeUnit;
import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class KeyValueStoreExample implements StreamGraphFactory {

  StreamSpec input1 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "PageViewEvent");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec output = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "PageViewPerMember5min");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  class PageViewEvent {
    String pageId;
    String memberId;
    long timestamp;
  }

  class StatsOutput implements MessageEnvelope<String, StatsOutput.OutputRecord> {

    class OutputRecord {
      private String key;
      private long timestamp;
      private Integer count;
    }

    final OutputRecord record;

    StatsOutput(String key, long timestamp, Integer count) {
      this.record = new OutputRecord();
      this.record.key = key;
      this.record.timestamp = timestamp;
      this.record.count = count;
    }

    @Override
    public String getKey() {
      return this.record.key;
    }

    @Override
    public OutputRecord getMessage() {
      return this.record;
    }
  }

  class MyStatsCounter implements MapFunction<JsonMessageEnvelope, StatsOutput> {
    private final Integer TIMEOUT_MS = 10*60*1000;

    KeyValueStore<String, StatsWindowState> statsStore;

    class StatsWindowState {
      int lastCount = 0;
      long timeAtLastOutput = 0;
      int newCount = 0;
    }

    @Override
    public StatsOutput apply(JsonMessageEnvelope message) {
      long wndTimestamp = (long) Math.floor(TimeUnit.MILLISECONDS.toMinutes(message.getMessage().timestamp) / 5) * 5;
      String wndKey = String.format("%s-%d", message.getMessage().memberId, wndTimestamp);
      StatsWindowState curState = this.statsStore.get(wndKey);
      curState.newCount++;
      if (curState.newCount > 0 && curState.timeAtLastOutput + TIMEOUT_MS < System.currentTimeMillis()) {
        curState.timeAtLastOutput = System.currentTimeMillis();
        curState.lastCount += curState.newCount;
        curState.newCount = 0;
        StatsOutput newStats = new StatsOutput(message.getMessage().memberId, wndTimestamp, curState.lastCount);
        this.statsStore.put(wndKey, curState);
        return newStats;
      }
      // update counter w/o generating output
      this.statsStore.put(wndKey, curState);
      return null;
    }

    @Override
    public void init(Config config, TaskContext context) {
      this.statsStore = (KeyValueStore<String, StatsWindowState>) context.getStore("my-stats-wnd-store");
    }
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<PageViewEvent> {
    JsonMessageEnvelope(String key, PageViewEvent data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  /**
   * used by remote execution environment to launch the job in remote program. The remote program should follow the similar
   * invoking context as in standalone:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ExecutionEnvironment remoteEnv = ExecutionEnvironment.getRemoteEnvironment(config);
   *     UserMainExample runnableApp = new UserMainExample();
   *     runnableApp.run(remoteEnv, config);
   *   }
   *
   */
  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);

    graph.<String, PageViewEvent, JsonMessageEnvelope>createInStream(
        input1, new StringSerde("UTF-8"), new JsonSerde<>()).
        partitionBy(m -> m.getMessage().memberId).
        map(new MyStatsCounter()).
        sendTo(graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>()));

    return graph;
  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new KeyValueStoreExample(), config);
  }

}
