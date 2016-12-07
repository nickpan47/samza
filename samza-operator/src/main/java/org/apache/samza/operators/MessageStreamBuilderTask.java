package org.apache.samza.operators;

import org.apache.samza.Partition;
import org.apache.samza.operators.data.IncomingSystemMessageEnvelope;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;

import java.util.*;


/**
 * Created by yipan on 12/6/16.
 */
public class MessageStreamBuilderTask implements StreamOperatorTask {
  private final MessageStreamBuilder streamBuilder;
  private final Map<SystemStream, Map<Partition, MessageStream<IncomingSystemMessageEnvelope>>> inputBySystemStream = new HashMap<>();
  private final Map<MessageStream, MessageStreamImpl> intermediateStreamsMap = new HashMap<>();

  public MessageStreamBuilderTask(MessageStreamBuilder streamBuilder) {
    this.streamBuilder = streamBuilder;
  }

  public void transform(Map<SystemStreamPartition, MessageStream<IncomingSystemMessageEnvelope>> streams) {
    // use {@code streamBuilder} as the template to instantiate the actual program
    streams.forEach((ssp, mstream) -> {
      this.inputBySystemStream.putIfAbsent(ssp.getSystemStream(), new HashMap<>());
      this.inputBySystemStream.get(ssp.getSystemStream()).putIfAbsent(ssp.getPartition(), mstream);
    });
    this.inputBySystemStream.forEach((ss, parMap) -> {
      createMessageStreamClone(ss, parMap);
    });
  }

  private void createMessageStreamClone(SystemStream ss, Map<Partition, MessageStream<IncomingSystemMessageEnvelope>> parMap) {
    // Here we will assume that the program is at {@link SystemStream} level. Hence, any two partitions from the same {@link SystemStream}
    // that are assigned (grouped) in the same task will be "merged" to the same operator instances that consume the {@link SystemStream}

      Collection<OperatorSpec> curSubscribers = ((MessageStreamImpl<IncomingSystemMessageEnvelope>)this.inputBySystemStream.get(ssp.getSystemStream())).getRegisteredOperatorSpecs();
    MessageStreamImpl<IncomingSystemMessageEnvelope> mergedStream = (MessageStreamImpl<IncomingSystemMessageEnvelope>)
          mstream.merge(new ArrayList<MessageStream<IncomingSystemMessageEnvelope>>() { {
            this.add(MessageStreamBuilderTask.this.inputBySystemStream.get(ssp.getSystemStream()));
          } });
      curSubscribers.forEach(opSpec -> MessageStreamImpl.switchSource((MessageStreamImpl)this.inputBySystemStream.get(ssp.getSystemStream()), mergedStream, opSpec));
      this.inputBySystemStream.put(ssp.getSystemStream(), mergedStream);
      return;
    // Now create new instance of opSpecs and following output {@link MessageStreamImpl}
    Collection<OperatorSpec> subscribers = ((MessageStreamImpl<IncomingSystemMessageEnvelope>)messageStream).getRegisteredOperatorSpecs();
    subscribers.forEach(subOpSpec -> {
      MessageStreamImpl prevStream = this.intermediateStreamsMap.putIfAbsent(subOpSpec.getOutputStream(), ((MessageStreamImpl)subOpSpec.getOutputStream()).clone());
      OperatorSpec opClone = subOpSpec.clone(this.intermediateStreamsMap.get(subOpSpec.getOutputStream()));
      ((MessageStreamImpl) mstream).addSubscriber(opClone);
      if (prevStream == null) {
        // only traverse further if the intermediate stream is created the first time
        Collection<OperatorSpec> newSubs = ((MessageStreamImpl) prevStream).getRegisteredOperatorSpecs();
        newSubs.forEach(newSubSpec -> {

        });
      }
    });
  }
}
