package org.apache.samza.operators.functions;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 1/11/17.
 */
@InterfaceStability.Unstable
public interface ContextInitFunction {
  void init(Config config, TaskContext context);
}
