package org.apache.samza.operators.functions;

import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 1/25/17.
 */
public interface InitFunction {
  void init(Config config, TaskContext context);
}
