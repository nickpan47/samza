package org.apache.samza.example;

import org.apache.samza.config.AppConfig;
import org.apache.samza.config.Config;
import org.apache.samza.system.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.apache.samza.util.CommandLine;

/**
 * Created by yipan on 3/2/17.
 */
public class MultipleStreamTaskExample {

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    try {
      localRunner.start(new FirstTaskFactory(), config);
      localRunner.start(new SecondTaskFactory(), config);
      while(localRunner.isRunning()) {
        Thread.sleep(10000);
      }
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      localRunner.stop();
    }
  }

  static class FirstTask implements StreamTask, InitableTask {

    @Override
    public void init(Config config, TaskContext context) throws Exception {

    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

    }
  }

  static class FirstTaskFactory implements StreamTaskFactory {

    @Override
    public StreamTask createInstance() {
      return new FirstTask();
    }
  }

  static class SecondTask implements StreamTask, WindowableTask {

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

    }
  }

  static class SecondTaskFactory implements StreamTaskFactory {

    @Override
    public StreamTask createInstance() {
      return new SecondTask();
    }
  }
}
