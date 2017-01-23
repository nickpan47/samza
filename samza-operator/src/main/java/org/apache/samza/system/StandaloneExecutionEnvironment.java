package org.apache.samza.system;

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreams;
import org.apache.samza.operators.MessageStreamsImpl;
import org.apache.samza.task.StreamTask;


/**
 * Created by yipan on 1/5/17.
 */
public class StandaloneExecutionEnvironment implements ExecutionEnvironment {

  @Override public MessageStreams createGraph() {
    return new MessageStreamsImpl();
  }

  @Override public void run(MessageStreams graph) {
    // TODO: actually instantiate the tasks and run the job, i.e.
    // 1. create all input/output/intermediate topics
    // 2. create the configuration for StreamProcessor
    // 3. start the StreamProcessor
  }

  @Override public void runTask(StreamTask streamTask, Config config) {
    // 1. create all input/output/intermediate topics
    // 2. create the single job configuration, w/ task.class=streamTask.getClass().getName()
    // 3. execute JobRunner to submit the single job for the whole graph
  }
}
