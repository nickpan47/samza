package org.apache.samza.operators;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.StreamSpec;

/**
 * Created by yipan on 6/11/17.
 */
public class StreamIO {

  public static <K, V> Input<K, V> read(String streamId) {
    return new Input<K, V>();
  }

  public static <K, V> Input<K, V> read(String streamId, Class<K> keyClass, Class<V> valueClass) {
    return new Input<K, V>();
  }

  public static <K, V> Output<K, V> write(String streamId) {
    return new Output<K, V>();
  }

  public static <K, V> Output<K, V> write(String streamId, Class<K> keyClass, Class<V> valueClass) {
    return new Output<K, V>();
  }

  public static class Input<K, V> extends StreamSpec {
    private IOSystem inputSystem;
    private Serde<K> keySerde;
    private Serde<V> msgSerde;

    public Input<K, V> withKeySerde(Serde<K> keySerde) {
      this.keySerde = keySerde;
      return this;
    }

    public Input<K, V> withMsgSerde(Serde<V> msgSerde) {
      this.msgSerde = msgSerde;
      return this;
    }

    public Input<K, V> from(IOSystem system) {
      this.inputSystem = system;
      return this;
    }
  }

  public static class Output<K, V> extends StreamSpec {
    private IOSystem outputSystem;
    private Serde<K> keySerde;
    private Class<K> keyType;
    private Serde<V> msgSerde;
    private Class<V> msgType;

    public Output<K, V> withKeySerde(Serde<K> keySerde) {
      this.keySerde = keySerde;
      return this;
    }

    public Output<K, V> withKeyType(Class<K> keyType) {
      this.keyType = keyType;
      return this;
    }

    public Output<K, V> withMsgSerde(Serde<V> msgSerde) {
      this.msgSerde = msgSerde;
      return this;
    }

    public Output<K, V> withMsgType(Class<V> msgType) {
      this.msgType = msgType;
      return this;
    }

    public Output<K, V> to(IOSystem system) {
      this.outputSystem = system;
      return this;
    }
  }
}
