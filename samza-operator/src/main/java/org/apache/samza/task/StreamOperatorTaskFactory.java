package org.apache.samza.task;

import org.apache.samza.operators.MessageStreamsBuilder;
import org.apache.samza.operators.MessageStreamsBuilderTask;
import org.apache.samza.operators.StreamOperatorAdaptorTask;


/**
 * Created by yipan on 12/11/16.
 */
public class StreamOperatorTaskFactory implements StreamTaskFactory {
  private final MessageStreamsBuilder messageStreamsBuilder;

  public StreamOperatorTaskFactory(MessageStreamsBuilder streamsBuilder) {
    this.messageStreamsBuilder = streamsBuilder;
  }

  @Override public StreamTask createInstance() {
    return new StreamOperatorAdaptorTask(new MessageStreamsBuilderTask(this.messageStreamsBuilder));
  }
}
