package org.apache.samza.operators.spec;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.data.MessageEnvelope;

import java.util.function.Function;


/**
 * This operator spec should only exist in the logic graph. When a specific {@link org.apache.samza.system.ExecutionEnvironment}
 * translate the logic graph to a physical graph, this operator is either translated into a pass-through {@link StreamOperatorSpec},
 * or a physical {@link SinkOperatorSpec} and an intermediate {@link org.apache.samza.operators.MessageStreamImpl} needs
 * to be added to the {@link org.apache.samza.operators.MessageStreamGraphImpl}.
 */
public class PartitionOperatorSpec<K, M extends MessageEnvelope> implements OperatorSpec<M> {

  private final MessageStreamImpl<M> outputStream;

  private final Function<M, K> parKeyExtractor;

  PartitionOperatorSpec(Function<M, K> parKeyExtractor, MessageStreamImpl<M> output) {
    this.parKeyExtractor = parKeyExtractor;
    this.outputStream = output;
  }

  @Override public MessageStreamImpl<M> getOutputStream() {
    return this.outputStream;
  }

  public Function<M, K> getParKeyFunction() {
    return this.parKeyExtractor;
  }
}
