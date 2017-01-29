package org.apache.samza.application;

import org.apache.samza.operators.StreamGraph;


/**
 * Created by yipan on 1/24/17.
 */
public interface StreamGraphBuilder extends AutoCloseable {
  StreamGraph createGraph();
}
