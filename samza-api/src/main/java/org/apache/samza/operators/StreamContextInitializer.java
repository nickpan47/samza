package org.apache.samza.operators;

import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 1/9/17.
 */
@FunctionalInterface
public interface StreamContextInitializer {
  StreamContext init(Config config, TaskContext context);
}
