package org.apache.samza.operators.functions;

import org.apache.samza.config.Config;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 1/10/17.
 */
public interface PartialJoinFunctionWithContext<M extends MessageEnvelope, OM extends MessageEnvelope, RM extends MessageEnvelope> {
  RM apply(M m1, OM om);

  void init(Config config, TaskContext context);
}
