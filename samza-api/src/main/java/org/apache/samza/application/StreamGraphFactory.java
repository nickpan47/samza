package org.apache.samza.application;

import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamGraph;


/**
 * Created by yipan on 1/24/17.
 */
public interface StreamGraphFactory {
  /**
   * Users are required to implement this abstract method to initialize the processing logic of the application, in terms
   * of a DAG of {@link org.apache.samza.operators.MessageStream}s and operators
   *
   * @param config  the {@link Config} of the application
   */
  StreamGraph create(Config config);
}
