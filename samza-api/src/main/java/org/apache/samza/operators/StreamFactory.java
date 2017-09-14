package org.apache.samza.operators;

/**
 * Created by yipan on 9/15/17.
 */
public interface StreamFactory {
  <K, V> StreamDescriptor.Input<K, V> getInputStreamDescriptor(String streamId);
  <K, V> StreamDescriptor.Output<K, V> getOutputStreamDescriptor(String streamId);
}
