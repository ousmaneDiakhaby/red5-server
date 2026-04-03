package org.red5.server.stream;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.red5.server.api.stream.ISubscriberStream;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;

public class PlaybackContext {

  public final ISubscriberStream subscriberStream;

  public final Number streamId;

  public final AtomicReference<IMessageInput> messageInput;

  public final AtomicReference<IMessageOutput> messageOutput;

  public final AtomicInteger streamStartTimestamp = new AtomicInteger(-1);

  public volatile boolean configsDone = false;

  public int timestampOffset = 0;

  public int streamOffset = 0;

  public PlaybackContext(ISubscriberStream subscriberStream, AtomicReference<IMessageInput> messageInput, AtomicReference<IMessageOutput> messageOutput) {
    this.subscriberStream = subscriberStream;
    this.streamId = subscriberStream.getStreamId();
    this.messageInput = messageInput;
    this.messageOutput = messageOutput;
  }

}
