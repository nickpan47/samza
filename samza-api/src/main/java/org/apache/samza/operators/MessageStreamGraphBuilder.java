package org.apache.samza.operators;

import org.apache.samza.config.Config;
import org.apache.samza.system.ExecutionEnvironment;


/**
 * Created by yipan on 1/5/17.
 */
public class MessageStreamGraphBuilder {
  public static MessageStreamGraph fromConfig(Config config) {
    try {
      ExecutionEnvironment env = (ExecutionEnvironment) Class.forName(config.get("execution.environment.class")).newInstance();
      return env.initGraph(config);
    } catch (Throwable t) {
      throw new IllegalArgumentException(t);
    }
  }
}
