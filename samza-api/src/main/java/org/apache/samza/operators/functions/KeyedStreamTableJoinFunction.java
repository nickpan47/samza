package org.apache.samza.operators.functions;

import org.apache.samza.operators.KV;


/**
 * Created by yipan on 11/27/17.
 */
public interface KeyedStreamTableJoinFunction<K, V, R, JM> extends StreamTableJoinFunction<K, KV<K, V>, KV<K, R>, JM> {

  default K getMessageKey(KV<K, V> message) {
    return message.getKey();
  }

  default K getRecordKey(KV<K, R> record) {
    return record.getKey();
  }
}
