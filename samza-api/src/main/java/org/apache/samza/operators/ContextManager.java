package org.apache.samza.operators;

import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


public interface ContextManager {
  default void initTaskContext(Config config, TaskContext context) {};
  default void finalizeTaskContext() {};
}
