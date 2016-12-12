package org.apache.samza.processor;

import org.apache.samza.operators.MessageStreamsBuilder;
import org.apache.samza.operators.MessageStreamsBuilderImpl;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.data.JsonIncomingSystemMessageEnvelope;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.StreamOperatorTaskFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class TestMessageStreamsBuilderBasedProcessor {
  @Test
  public void constructProcessorViaMessageStreamsBuilder() {
    MessageStreamsBuilder streamsBuilder = new MessageStreamsBuilderImpl();
    streamsBuilder.<IncomingSystemMessageEnvelope>addInputStream(new SystemStream("kafka", "mytopic-1")).
        join(streamsBuilder.<IncomingSystemMessageEnvelope>addInputStream(new SystemStream("kafka", "mytopic-2")),
            (m1, m2) -> new JsonIncomingSystemMessageEnvelope<>(m1.getKey().toString(),
                new HashMap<Object, Object>() {
                  {
                    this.putAll((Map<Object, Object>) m1.getMessage());
                    this.putAll((Map<Object, Object>) m2.getMessage());
                  }
                }, null, null)).
        window(Windows.intoSessionCounter(m -> String.format("%s", m.getMessage().get("treeId")))).
        sink(new SystemStream("kafka", "intermediate-1"));
    StreamProcessor streamProcessor = new StreamProcessor(0, null, null, new StreamOperatorTaskFactory(streamsBuilder));
    streamProcessor.start();
    try {
      streamProcessor.awaitStart(500000);
    } catch(Throwable t) {
      // ignore...
    } finally {
      streamProcessor.stop();
    }
  }
}
