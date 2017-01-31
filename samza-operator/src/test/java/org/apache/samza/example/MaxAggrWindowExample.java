package org.apache.samza.example;

import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CommandLine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Created by yipan on 1/26/17.
 */
public class MaxAggrWindowExample implements StreamGraphFactory {

  StreamSpec input = new StreamSpec() {
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

  class MessageType {
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  class MyStreamOutput implements MessageEnvelope<String, MyStreamOutput.OutputRecord> {
    WindowPane<Void, Integer> wndOutput;

    class OutputRecord {
      long timestamp;
      int count;
    }

    OutputRecord record;

    MyStreamOutput(WindowPane<Void, Integer> m) {
      this.wndOutput = m;
      this.record.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.record.count = m.getMessage();
    }

    @Override
    public String getKey() {
      return new Long(this.record.timestamp).toString();
    }

    @Override
    public OutputRecord getMessage() {
      return this.record;
    }
  }

  Integer getMaxFieldsLength(JsonMessageEnvelope m, Integer curMaxLen) {
    if (curMaxLen == null) {
      return m.getMessage().joinFields.size();
    }
    return m.getMessage().joinFields.size() > curMaxLen ? m.getMessage().joinFields.size() : curMaxLen;
  }

  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);

    graph.<String, MessageType, JsonMessageEnvelope>createInStream(input, new StringSerde("UTF-8"), new JsonSerde<>()).
        <Void, Integer, WindowPane<Void, Integer>>window(Windows.tumblingWindow(Duration.ofSeconds(10), this::getMaxFieldsLength).
            setEarlyTrigger(Triggers.repeat(Triggers.count(5))).
            setAccumulationMode(AccumulationMode.DISCARDING)).
        map(m -> new MyStreamOutput(m)).
        sendTo(graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>()));
    return graph;
  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new WaterlooEducationExample(), config);
  }
}
