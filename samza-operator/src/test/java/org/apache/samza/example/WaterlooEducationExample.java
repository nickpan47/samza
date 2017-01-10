package org.apache.samza.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.config.Config;
import org.apache.samza.operators.*;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.CommandLine;

import java.util.*;


public class WaterlooEducationExample implements MessageStreamApplication {

  StreamSpec input1 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "input1");
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec input2 = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return new SystemStream("kafka", "input2");
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

  class StandardizeContext implements StreamContext {

    @Override public <K, V> KeyValueStore<K, V> getStore(String store) {
      return null;
    }

    @Override public <C> C getUserContext() {
      return null;
    }

    @Override public TaskContext getTaskContext() {
      return null;
    }

    StandardizeContext(Config config, TaskContext context) {

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

    public TaskMetrics _metrics;
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

  class SerializeContext implements StreamContext {
    SerializeContext(Config config, TaskContext context) {
    }

    @Override public <K, V> KeyValueStore<K, V> getStore(String store) {
      return null;
    }

    @Override public <C> C getUserContext() {
      return null;
    }

    @Override public TaskContext getTaskContext() {
      return null;
    }

    MessageEnvelope<String, OutgoingMessageEnvelope> getOutgoingMessage(int profileId, StandardizedEducation stdEducation) {
      return new MessageEnvelope<String, OutgoingMessageEnvelope>() {
        @Override public String getKey() {
          return null;
        }

        @Override public OutgoingMessageEnvelope getMessage() {
          return null;
        }
      };
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
  @Override public void run(ExecutionEnvironment env, Config config) {
    try {
      MessageStreamGraph graph = env.initGraph(config);

      MessageStream<IncomingAvroMessageEnvelope> stream = graph.addInStream(input1, new StringSerde("UTF-8"), null);
      stream.map(m -> this.genericRecordToSpecificRecord(new Profile(), m.getMessage())).
          filter(p -> !(p.educations == null || p.educations.isEmpty())).
          map((p, c) -> {
              StandardizeContext sc = (StandardizeContext) c;
              for (Map.Entry<CharSequence, Education> educationEntry : p.educations.entrySet()) {
                Education education = educationEntry.getValue();
                Integer schoolId = sc.standardizeSchool( p.id, education, sc.defaultLocale);
                StandardizedDegree stdDegree = sc.standardizeDegree(education, sc.defaultLocale);
                List<StandardizedFieldOfStudy> stdFieldsOfStudy =
                    sc.standardizeFieldOfStudy(education, sc.defaultLocale);

                p.memberStandardizedEducation.educations
                    .add(toMemberEducationStandardizedValues(education, schoolId, stdDegree, stdFieldsOfStudy));
                sc._metrics.incNumEducationsProcessed();
              }
              return p;
            },
            (conf, context) -> new StandardizeContext(conf, context) {}).
          map((p, c) -> {
                SerializeContext serC = (SerializeContext) c;
                return serC.getOutgoingMessage(p.id, p.memberStandardizedEducation);
              },
              (conf, context) -> new SerializeContext(conf, context) {}).
          sink((p, mc, t) -> mc.send(p.getMessage()));

      env.run(graph);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  // standalone local program model
  public static void main(String args[]) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    WaterlooEducationExample runnableApp = new WaterlooEducationExample();
    runnableApp.run(standaloneEnv, config);
  }

}
