package org.apache.samza.operators;

import org.apache.samza.config.Config;
import org.apache.samza.system.ExecutionEnvironment;
import org.junit.Test;

import static org.mockito.Mockito.mock;


/**
 * Created by yipan on 1/5/17.
 */
public class TestMessageStreamGraphBuilder {

  @Test
  public void testGetGraph() {
    Config mockConfig = mock(Config.class);
    MessageStreamGraph msgGraph = MessageStreamGraphBuilder.fromConfig(mockConfig);
    msgGraph.run(ExecutionEnvironment.RuntimeEnvironment.STANDALONE);
  }
}
