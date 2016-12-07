package org.apache.samza.operators;

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.system.SystemStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by yipan on 12/6/16.
 */
public class MessageStreamBuilderImpl implements MessageStreamBuilder {
  private final Map<SystemStream, MessageStream> inputStreamsMap = new HashMap<>();

  @Override public <M extends MessageEnvelope> MessageStream<M> addInputStream(SystemStream input) {
    inputStreamsMap.putIfAbsent(input, new MessageStreamImpl<>());
    return inputStreamsMap.get(input);
  }

  @Override public Map<SystemStream, MessageStream> getAllInputStreams() {
    return Collections.unmodifiableMap(inputStreamsMap);
  }
}
