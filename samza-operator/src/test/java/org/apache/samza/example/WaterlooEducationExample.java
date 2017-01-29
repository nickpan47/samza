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

import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.CommandLine;
import sun.net.www.http.HttpClient;

import java.net.URL;
import java.util.*;


public class WaterlooEducationExample implements StreamGraphFactory {

  StreamSpec input1 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "input1");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec output = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "output");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  // mock GenericRecord and SpecificRecord to avoid pulling in avro dependencies in test code
  interface GenericRecord {

  }

  interface SpecificRecord {

  }

  class IncomingAvroMessageEnvelope implements MessageEnvelope<String, GenericRecord> {
    private final String key;
    private final GenericRecord record;

    IncomingAvroMessageEnvelope(String key, Object msg) {
      this.key = key;
      this.record = (GenericRecord) msg;
    }

    @Override public String getKey() {
      return this.key;
    }

    @Override public GenericRecord getMessage() {
      return this.record;
    }
  }

  class Education {

  }

  class Locale {

  }

  class Profile implements MessageEnvelope<String, SpecificRecord> {
    private String key;
    private SpecificRecord record;

    public int id;
    public StandardizedEducation memberStandardizedEducation;

    public Map<CharSequence, Education> educations = new HashMap<>();

    Profile() {
    }

    @Override public String getKey() {
      return this.key;
    }

    @Override public SpecificRecord getMessage() {
      return this.record;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public void setRecord(GenericRecord record) {
      this.record = (SpecificRecord) record;
    }
  }

  Profile genericRecordToSpecificRecord(Profile profile, GenericRecord record) {
    return profile;
  }



  class TaskMetrics {
    public void incNumEducationsProcessed() {

    }
  }

  class StandardizedDegree {

  }

  class StandardizedFieldOfStudy {

  }

  class StandardizedEducation {
    List<Education> educations;
  }

  Education toMemberEducationStandardizedValues(Education education, int schoolId, StandardizedDegree stdDegree, List<StandardizedFieldOfStudy> stdFieldsOfStudy) {
    return education;
  }

  class SystemOutgoingMessageEnvelope implements MessageEnvelope<Object, Object> {
    private final OutgoingMessageEnvelope envelope;

    SystemOutgoingMessageEnvelope(OutgoingMessageEnvelope envelope) {
      this.envelope = envelope;
    }

    @Override public Object getKey() {
      return this.envelope.getKey();
    }

    @Override public Object getMessage() {
      return this.envelope.getMessage();
    }

    public OutgoingMessageEnvelope getEnvelope() {
      return this.envelope;
    }
  }


  class SerializedMap implements MapFunction<Profile, SystemOutgoingMessageEnvelope> {

    private final SystemStream outputStream = new SystemStream("kafka", "waterloo-education-standardizer");

    SystemOutgoingMessageEnvelope getOutgoingMessage(int profileId, StandardizedEducation stdEducation) {
      return new SystemOutgoingMessageEnvelope(new OutgoingMessageEnvelope(this.outputStream, profileId, stdEducation));
    }

    @Override public SystemOutgoingMessageEnvelope apply(Profile p) {
      return this.getOutgoingMessage(p.id, p.memberStandardizedEducation);
    }

    @Override public void init(Config config, TaskContext context) {

    }
  }

  class StandardizeEducationMap implements MapFunction<Profile, Profile> {

    private KeyValueStore<Integer, Profile> kvStore = null;
    private Map<Integer, Profile> mapInTask = null;


    StandardizeEducationMap() {
    }

    private Integer readVoldemortSchoolTable(int profileId, Education education, Locale defaultLocale) {
      return 0;
    }

    Integer standardizeSchool(int profileId, Education education, Locale defaultLocale) {
      return readVoldemortSchoolTable(profileId, education, defaultLocale);
    }

    StandardizedDegree standardizeDegree(Education education, Locale defaultLocale) {
      return new StandardizedDegree();
    }

    List<StandardizedFieldOfStudy> standardizeFieldOfStudy(Education education, Locale defaultLocale) {
      return new ArrayList<>();
    }

    public Locale defaultLocale = new Locale();

    public TaskMetrics metrics;

    @Override public Profile apply(Profile p) {
      for (Map.Entry<CharSequence, Education> educationEntry : p.educations.entrySet()) {
        Education education = educationEntry.getValue();
        Integer schoolId = this.standardizeSchool(p.id, education, this.defaultLocale);
        StandardizedDegree stdDegree = this.standardizeDegree(education, this.defaultLocale);
        List<StandardizedFieldOfStudy> stdFieldsOfStudy =
            this.standardizeFieldOfStudy(education, this.defaultLocale);

        p.memberStandardizedEducation.educations
            .add(toMemberEducationStandardizedValues(education, schoolId, stdDegree, stdFieldsOfStudy));
        this.metrics.incNumEducationsProcessed();
      }
      // save to kv store and map
      this.kvStore.put(p.id, p);
      this.mapInTask.putIfAbsent(p.id, p);
      return p;
    }

    @Override public void init(Config config, TaskContext context) {
      this.kvStore = (KeyValueStore<Integer, Profile>) context.getStore("my-kvstore");
      this.mapInTask = context.<Map<Integer, Profile>>getUserDefinedContext();
    }
  }

  class EducationSinkFunction implements SinkFunction<SystemOutgoingMessageEnvelope> {

    private HttpClient restliClient;

    EducationSinkFunction() {

    }

    @Override public void apply(SystemOutgoingMessageEnvelope message, MessageCollector messageCollector,
        TaskCoordinator taskCoordinator) {
      messageCollector.send(message.getEnvelope());
      // this.restliClient.writeRequests(new MessageHeader(), null);
    }

    public void init(Config config, TaskContext context) {
      this.restliClient = context.getSamzaContainerContext().<HttpClient>getUserDefinedContext();
    }

  }

  // global resource for the whole JVM process of a Samza job
  HttpClient restLiClient;

  /**
   * used by remote execution environment to launch the job in remote program. The remote program should follow the similar
   * invoking context as in standalone:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ExecutionEnvironment remoteEnv = ExecutionEnvironment.fromConfig(config);
   *     remoteEnv.run(StreamApplication.fromConfig(config), config);
   *   }
   *
   */
  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);
    MessageStream<IncomingAvroMessageEnvelope> stream = graph.createInStream(input1, new StringSerde("UTF-8"), null);
    stream.map(m -> WaterlooEducationExample.this.genericRecordToSpecificRecord(new Profile(), m.getMessage())).
        filter(p -> !(p.educations == null || p.educations.isEmpty())).
        map(new StandardizeEducationMap()).
        map(new SerializedMap()).
        sink(new EducationSinkFunction());
    graph.withInitialer(new ContextManager() {
      TaskContext initTaskContext(config, context) {
        map = new HashMap<>();
        context.setUserDefinedContext(map);
        return context;
      }

      void closeTaskContext(TaskContext context) {
        ///
      }

//      SamzaContainerContext initContainerContext(config, context) {
//        return context;
//      }
    });
    return graph;
  }

  public void start(Config config) {
    try {
      // global resource for the whole JVM process of a Samza job
      this.restLiClient = HttpClient.New(new URL("http", "localhost", 8000, "/"));
    } catch (Exception e) {

    }
  }

  public void stop() {
    this.restLiClient.closeIdleConnection();
  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new WaterlooEducationExample(), config);
  }

}
