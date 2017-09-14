package org.apache.samza.system.kafka;

import org.apache.samza.config.Config;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.StreamFactory;


/**
 * Created by yipan on 9/15/17.
 */
public class KafkaStreamFactory implements StreamFactory {

  private final KafkaSystem system;

  private KafkaStreamFactory(String name) {
    this.system = KafkaSystem.create(name);
  }

  public static KafkaStreamFactory create(String name) {
    return new KafkaStreamFactory(name);
  }

  public KafkaStreamFactory withBootstrapServers(String servers) {
    this.system.withBootstrapServers(servers);
    return this;
  }

  public KafkaStreamFactory withConsumerProperties(Config consumerConfig) {
    this.system.withConsumerProperties(consumerConfig);
    return this;
  }

  public KafkaStreamFactory withProducerProperties(Config producerConfig) {
    this.system.withProducerProperties(producerConfig);
    return this;
  }

  @Override
  public <K, V> StreamDescriptor.Input<K, V> getInputStreamDescriptor(String streamId) {
    return StreamDescriptor.input(streamId);
  }

  @Override
  public <K, V> StreamDescriptor.Output<K, V> getOutputStreamDescriptor(String streamId) {
    return StreamDescriptor.output(streamId);
  }
}
