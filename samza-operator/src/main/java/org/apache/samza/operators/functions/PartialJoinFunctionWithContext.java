package org.apache.samza.operators.functions;

import org.apache.samza.operators.StreamContext;
import org.apache.samza.operators.data.MessageEnvelope;


/**
 * Created by yipan on 1/10/17.
 */
@FunctionalInterface
public interface PartialJoinFunctionWithContext<M extends MessageEnvelope, OM extends MessageEnvelope, RM extends MessageEnvelope> {
  RM apply(M m1, OM om, StreamContext context);
}
