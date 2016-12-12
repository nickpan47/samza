/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.job

import org.apache.samza.SamzaException
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.{Config, ConfigRewriter}
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory
import org.apache.samza.coordinator.stream.messages.{Delete, SetConfig}
import org.apache.samza.job.ApplicationStatus.{Running, SuccessfulFinish}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.pipeline.DotPipelineFactory
import org.apache.samza.pipeline.api.PipelineFactory
import org.apache.samza.pipeline.{PipelineRunner, SingleStagePipelineFactory}
import org.apache.samza.util.{CommandLine, Logging, Util}

import scala.collection.JavaConversions._


object JobRunner extends Logging {
  val SOURCE = "job-runner"

  /**
   * Re-writes configuration using a ConfigRewriter, if one is defined. If
   * there is no ConfigRewriter defined for the job, then this method is a
   * no-op.
   *
   * @param config The config to re-write.
   */
  def rewriteConfig(config: Config): Config = {
    def rewrite(c: Config, rewriterName: String): Config = {
      val klass = config
              .getConfigRewriterClass(rewriterName)
              .getOrElse(throw new SamzaException("Unable to find class config for config rewriter %s." format rewriterName))
      val rewriter = Util.getObj[ConfigRewriter](klass)
      info("Re-writing config with " + rewriter)
      rewriter.rewrite(rewriterName, c)
    }

    config.getConfigRewriters match {
      case Some(rewriters) => rewriters.split(",").foldLeft(config)(rewrite(_, _))
      case _ => config
    }
  }

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val operationOpt = cmdline.parser.accepts("operation", "The operation to perform; run, status, kill.")
        .withRequiredArg
        .ofType(classOf[java.lang.String])
        .describedAs("operation=run")
        .defaultsTo("run") // TODO consider using stricter types by using joptsimple.ValueConverter
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)

    // TODO this is just a hack to use the pipeline runner
    val configPipelineFactory = "job.pipeline.factory.class"
    val configPipelineFile = "job.pipeline.file"
    val factory = if (config.containsKey(configPipelineFactory)) {
      config.getNewInstance(configPipelineFactory).asInstanceOf[PipelineFactory]
    } else if (config.containsKey(configPipelineFile)) {
      new DotPipelineFactory
    } else {
      new SingleStagePipelineFactory
    }
    val pipeline = factory.create(config)
    val runner = new PipelineRunner(pipeline, config)

    // TODO this option should be documented if it will be supported long term.
    val operation = options.valueOf(operationOpt)
    info("%s operation parsed." format operation)
    if (operation.equalsIgnoreCase("kill")) {
      runner.kill()
    } else if (operation.equalsIgnoreCase("status")) {
      // This is the only operation that requires a return value. Print the status to stdout.
      println(runner.status())
    } else if (operation.equalsIgnoreCase("run")) {
      runner.run()
    } else {
      cmdline.parser.printHelpOn(System.err)
      throw new SamzaException("Invalid job runner operation: %s" format operation)
    }
  }
}

/**
 * ConfigRunner is a helper class that sets up and executes a Samza job based
 * on a config URI. The configFactory is instantiated, fed the configPath,
 * and returns a Config, which is used to execute the job.
 */
class JobRunner(config: Config) extends Logging {

  /**
   * This function submits the samza job.
   * @param resetJobConfig This flag indicates whether or not to reset the job configurations when submitting the job.
   *                       If this value is set to true, all previously written configs to coordinator stream will be
   *                       deleted, and only the configs in the input config file will have an affect. Otherwise, any
   *                       config that is not deleted will have an affect.
   *                       By default this value is set to true.
   * @return The job submitted
   */
  def run(resetJobConfig: Boolean = true) = {
    debug("config: %s" format (config))
    val jobFactory: StreamJobFactory = getJobFactory
    val factory = new CoordinatorStreamSystemFactory
    val coordinatorSystemConsumer = factory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
    val coordinatorSystemProducer = factory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)

    // Create the coordinator stream if it doesn't exist
    info("Creating coordinator stream")
    val (coordinatorSystemStream, systemFactory) = Util.getCoordinatorSystemStreamAndFactory(config)
    val systemAdmin = systemFactory.getAdmin(coordinatorSystemStream.getSystem, config)
    systemAdmin.createCoordinatorStream(coordinatorSystemStream.getStream)

    if (resetJobConfig) {
      info("Storing config in coordinator stream.")
      coordinatorSystemProducer.register(JobRunner.SOURCE)
      coordinatorSystemProducer.start
      coordinatorSystemProducer.writeConfig(JobRunner.SOURCE, config)
    }
    info("Loading old config from coordinator stream.")
    coordinatorSystemConsumer.register
    coordinatorSystemConsumer.start
    coordinatorSystemConsumer.bootstrap
    coordinatorSystemConsumer.stop

    val oldConfig = coordinatorSystemConsumer.getConfig()
    if (resetJobConfig) {
      info("Deleting old configs that are no longer defined: %s".format(oldConfig.keySet -- config.keySet))
      (oldConfig.keySet -- config.keySet).foreach(key => {
        coordinatorSystemProducer.send(new Delete(JobRunner.SOURCE, key, SetConfig.TYPE))
      })
    }
    coordinatorSystemProducer.stop

    // Create the actual job, and submit it.
    val job = jobFactory.getJob(config).submit

    info("waiting for job to start")

    // Wait until the job has started, then exit.
    Option(job.waitForStatus(Running, 500)) match {
      case Some(appStatus) => {
        if (Running.equals(appStatus)) {
          info("job started successfully - " + appStatus)
        } else {
          warn("unable to start job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to start job successfully.")
    }

    info("exiting")
    job
  }

  def kill(): Unit = {
    val jobFactory: StreamJobFactory = getJobFactory

    // Create the actual job, and kill it.
    val job = jobFactory.getJob(config).kill()

    info("waiting for job to terminate")

    // Wait until the job has terminated, then exit.
    Option(job.waitForStatus(SuccessfulFinish, 5000)) match {
      case Some(appStatus) => {
        if (SuccessfulFinish.equals(appStatus)) {
          info("job terminated successfully - " + appStatus)
        } else {
          warn("unable to terminate job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to terminate job successfully.")
    }

    info("exiting")
  }

  def status(): ApplicationStatus = {
    val jobFactory: StreamJobFactory = getJobFactory

    // Create the actual job, and get its status.
    jobFactory.getJob(config).getStatus
  }

  private def getJobFactory: StreamJobFactory = {
    val jobFactoryClass = config.getStreamJobFactoryClass match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }
    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]
    info("job factory: %s" format (jobFactoryClass))
    jobFactory
  }
}
