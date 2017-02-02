package org.apache.samza.example;

import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
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
public class PageViewCounterExample implements StreamGraphFactory {

  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);

    MessageStream<JsonMessageEnvelope> pageViewEvents = graph.<String, PageViewEvent, JsonMessageEnvelope>createInStream(input1, new StringSerde("UTF-8"), new JsonSerde<>());
    MessageStream<MyStreamOutput> pageViewPerMemberCounters = graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>());

    pageViewEvents.
        window(Windows.<JsonMessageEnvelope, String, Integer>keyedTumblingWindow(m -> m.getMessage().memberId, Duration.ofSeconds(10), (m, c) -> c + 1).
            setEarlyTrigger(Triggers.repeat(Triggers.count(5))).
            setAccumulationMode(AccumulationMode.DISCARDING)).
        map(MyStreamOutput::new).
        sendTo(pageViewPerMemberCounters);
    return graph;
  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    standaloneEnv.run(new PageViewCounterExample(), config);
  }

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

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<PageViewEvent> {
    JsonMessageEnvelope(String key, PageViewEvent data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  class MyStreamOutput implements MessageEnvelope<String, MyStreamOutput.OutputRecord> {

    class OutputRecord {
      String memberId;
      long timestamp;
      int count;
    }

    MyStreamOutput.OutputRecord record;

    MyStreamOutput(WindowPane<String, Integer> m) {
      this.record.memberId = m.getKey().getKey();
      this.record.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.record.count = m.getMessage();
    }

    @Override
    public String getKey() {
      return this.record.memberId;
    }

    @Override
    public MyStreamOutput.OutputRecord getMessage() {
      return this.record;
    }
  }

}
