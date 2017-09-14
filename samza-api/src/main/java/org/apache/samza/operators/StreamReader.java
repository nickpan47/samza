package org.apache.samza.operators;

import java.util.Collection;
import org.apache.samza.system.IncomingMessageEnvelope;


/**
 * Created by yipan on 9/13/17.
 */
@FunctionalInterface
public interface StreamReader {
  Collection<IncomingMessageEnvelope> apply(IncomingMessageEnvelope ime);
}
