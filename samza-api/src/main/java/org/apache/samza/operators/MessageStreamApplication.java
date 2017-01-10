package org.apache.samza.operators;

import org.apache.samza.config.Config;
import org.apache.samza.system.ExecutionEnvironment;


/**
 * Created by yipan on 1/5/17.
 */
@FunctionalInterface
public interface MessageStreamApplication {
  void run(ExecutionEnvironment env, Config config);
}
