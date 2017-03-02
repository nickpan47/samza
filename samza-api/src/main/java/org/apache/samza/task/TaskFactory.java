package org.apache.samza.task;

/**
 * Created by yipan on 3/1/17.
 */
public interface TaskFactory<T> {
  T createInstance();
}
