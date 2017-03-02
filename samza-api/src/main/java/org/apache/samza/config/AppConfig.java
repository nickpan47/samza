package org.apache.samza.config;

/**
 * Created by yipan on 3/2/17.
 */
public class AppConfig extends MapConfig {
  private final Config config;
  public static final String APP_CLASS_CONFIG = "app.class";
  public static final String APP_RUNNER_CONFIG = "app.runner.class";
  public static final String DEFAULT_APP_RUNNER_CLASS = "org.apache.samza.system.AbstractApplicationRunner";

  public AppConfig(Config config) {
    this.config = config;
  }

  public String getAppClass() {
    return this.config.get(APP_CLASS_CONFIG);
  }

  public String getAppRunnerClass() {
    return this.config.get(APP_RUNNER_CONFIG, DEFAULT_APP_RUNNER_CLASS);
  }
}
