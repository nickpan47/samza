package org.apache.samza.config;

/**
 * Created by yipan on 3/2/17.
 */
public class StreamProcessorConfig extends MapConfig {
  private final Config config;

  public static final String PROCESSOR_ID_CONFIG = "job.processor.id";

  public StreamProcessorConfig(Config config) {
    this.config = config;
  }

  public int getProcessorId() {
    return this.config.getInt(PROCESSOR_ID_CONFIG, 0);
  }
}
