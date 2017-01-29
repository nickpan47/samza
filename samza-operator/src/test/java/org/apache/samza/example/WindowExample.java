package org.apache.samza.example;

import org.apache.samza.application.StreamGraphFactory;
import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamSpec;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.operators.windows.TriggerBuilder;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Created by yipan on 1/26/17.
 */
public class SessionMaxAggregationExample implements StreamGraphFactory {

  StreamSpec input = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return null;
    }

    @Override public Properties getProperties() {
      return null;
    }
  };

  StreamSpec output = new StreamSpec() {
    @Override public SystemStream getSystemStream() {
      return null;
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


  @Override public StreamGraph create(Config config) {
    StreamGraph graph = StreamGraph.fromConfig(config);

    graph.<Object, Object, IncomingSystemMessageEnvelope>createInStream(input, new StringSerde("UTF-8"), new JsonSerde<>()).
        map(m1 -> new JsonMessageEnvelope(this.myMessageKeyFunction(m1), (MessageType) m1.getMessage(), m1.getOffset(),
            m1.getSystemStreamPartition())).
        window(Windows.tumblingWindow(m -> m.getUserId(), Duration.ofSeconds(10))
                      .earlyTrigger(Triggers.repeat(Triggers.count(5)))
                      .setAccumulationMode(AccumulationMode.DISCARDING)).
        sendTo(graph.createOutStream(output, new StringSerde("UTF-8"), new IntegerSerde()));
    return graph;
  }

  String myMessageKeyFunction(MessageEnvelope<Object, Object> m) {
    return m.getKey().toString();
  }

  public static void main(String[] args) {
    //
  }

  //        window(Windows.<JsonMessageEnvelope, String>intoSessionCounter(
  //                  m -> String.format("%s-%s", m.getMessage().joinFields.get(0), m.getMessage().joinFields.get(1))).
  //                setTriggers(TriggerBuilder.<JsonMessageEnvelope, Integer>earlyTriggerWhenExceedWndLen(100).
  //                    addTimeoutSinceLastMessage(30000))).

}
