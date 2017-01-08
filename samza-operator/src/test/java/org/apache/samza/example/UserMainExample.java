package org.apache.samza.example;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.*;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.Offset;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class UserMainExample implements MessageStreamGraphFactory {

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

  class MessageType {
    String joinKey;
    List<String> joinFields = new ArrayList<>();
  }

  class JsonMessageEnvelope extends JsonIncomingSystemMessageEnvelope<MessageType> {
    JsonMessageEnvelope(String key, MessageType data, Offset offset, SystemStreamPartition partition) {
      super(key, data, offset, partition);
    }
  }

  private JsonMessageEnvelope getInputMessage(IncomingSystemMessageEnvelope ism) {
    return new JsonMessageEnvelope(
        ((MessageType) ism.getMessage()).joinKey,
        (MessageType) ism.getMessage(),
        ism.getOffset(),
        ism.getSystemStreamPartition());
  }

  JsonMessageEnvelope myJoinResult(JsonMessageEnvelope m1, JsonMessageEnvelope m2) {
    MessageType newJoinMsg = new MessageType();
    newJoinMsg.joinKey = m1.getKey();
    newJoinMsg.joinFields.addAll(m1.getMessage().joinFields);
    newJoinMsg.joinFields.addAll(m2.getMessage().joinFields);
    return new JsonMessageEnvelope(m1.getMessage().joinKey, newJoinMsg, null, null);
  }

  @Override public MessageStreamGraph apply(Config config) {
    try {
      MessageStreamGraph graph = MessageStreamGraphBuilder.fromConfig(config);

      MessageStream<JsonMessageEnvelope> newSource = graph.<Object, Object, IncomingSystemMessageEnvelope>addInStream(input1, null, null).map(this::getInputMessage);
      newSource.join(graph.<Object, Object, IncomingSystemMessageEnvelope>addInStream(input2, null, null).map(m -> this.getInputMessage(m)),
          (m1, m2) -> this.myJoinResult(m1, m2)).
          sink(output, new StringSerde("UTF-8"), new StringSerde("UTF-8"));

      return graph;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public void main(String args[]) throws Exception {
    Config processConfig = new MapConfig();
    MessageStreamGraph graph = apply(processConfig);
    graph.run(ExecutionEnvironment.RuntimeEnvironment.STANDALONE);
  }

}
