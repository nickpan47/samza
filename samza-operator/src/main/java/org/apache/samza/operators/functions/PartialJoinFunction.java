package org.apache.samza.operators.functions;

import org.apache.samza.operators.data.MessageEnvelope;


/**
 * Created by yipan on 1/10/17.
 */
public interface PartialJoinFunction<M extends MessageEnvelope, OM extends MessageEnvelope, RM extends MessageEnvelope> extends ContextInitFunction{
  RM apply(M m1, OM om);
}
