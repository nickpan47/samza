package org.apache.samza.operators;

import org.apache.samza.config.Config;


/**
 * Created by yipan on 1/5/17.
 */
@FunctionalInterface
public interface MessageStreamGraphFactory {
  MessageStreamGraph apply(Config config);
}
