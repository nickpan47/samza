package org.apache.samza.operators;

import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;


/**
 * Created by yipan on 1/9/17.
 */
public interface StreamContext {

  <K, V> KeyValueStore<K, V> getStore(String store);

  <C> C getUserContext();

  TaskContext getTaskContext();
}
