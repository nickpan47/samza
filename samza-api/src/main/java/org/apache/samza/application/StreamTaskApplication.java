package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.task.*;


/**
 * Created by yipan on 1/22/17.
 */
public abstract class StreamTaskApplication implements StreamTask, InitableTask, WindowableTask, ClosableTask {

  public final void run(ExecutionEnvironment env, Config config) {
    try {
      env.runTask(this, config);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override public void close() throws Exception {

  }

  @Override public void init(Config config, TaskContext context) throws Exception {

  }

  @Override public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

}
