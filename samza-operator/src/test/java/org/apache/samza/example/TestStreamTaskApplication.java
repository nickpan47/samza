package org.apache.samza.example;

import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.system.ExecutionEnvironment;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.CommandLine;


/**
 * Created by yipan on 1/22/17.
 */
public class TestStreamTaskApplication extends StreamTaskApplication {
  private final SystemStream ss = new SystemStream("outputSystem", "mytopic-1");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    collector.send(new OutgoingMessageEnvelope(ss, envelope.getKey(), envelope.getMessage()));
  }

  public static void main(String args[]) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ExecutionEnvironment standaloneEnv = ExecutionEnvironment.getLocalEnvironment(config);
    TestStreamTaskApplication runnableApp = new TestStreamTaskApplication();
    runnableApp.run(standaloneEnv, config);
  }
}
