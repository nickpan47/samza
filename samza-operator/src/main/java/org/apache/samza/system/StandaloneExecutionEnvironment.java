package org.apache.samza.system;

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamGraph;
import org.apache.samza.operators.MessageStreamGraphImpl;


/**
 * Created by yipan on 1/5/17.
 */
public class StandaloneExecutionEnvironment implements ExecutionEnvironment {

  @Override public MessageStreamGraph initGraph(Config config) {
    return new MessageStreamGraphImpl();
  }

  @Override public void run(MessageStreamGraph graph) {
    // TODO: actually instantiate the tasks and run the job, i.e.
    // 1. create all input/output/intermediate topics
    // 2. create the configuration for StreamProcessor
    // 3. start the StreamProcessor
  }
}
