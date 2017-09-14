package org.apache.samza.system.dali;

import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.StreamFactory;


/**
 * Created by yipan on 9/15/17.
 */
public class DaliStreamFactory implements StreamFactory {
  private final String name;
  private String viewMetastoreUrl;

  private DaliStreamFactory(String name) {
    this.name = name;
  }

  public static DaliStreamFactory create(String name) {
    return new DaliStreamFactory(name);
  }

  public DaliStreamFactory withViewMetadata(String url) {
    this.viewMetastoreUrl = url;
    return this;
  }

  @Override
  public <K, V> StreamDescriptor.Input<K, V> getInputStreamDescriptor(String streamId) {
    // Find StreamSpecs and Reader function from view metadata store
    // set it to the StreamDescriptor.Input
    return StreamDescriptor.<K, V>input(streamId);
  }

  @Override
  public <K, V> StreamDescriptor.Output<K, V> getOutputStreamDescriptor(String streamId) {
    return StreamDescriptor.<K, V>output(streamId);
  }
}
