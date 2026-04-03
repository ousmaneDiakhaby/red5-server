package org.red5.server.stream.strategy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.StreamState;
import org.red5.server.messaging.IConsumer;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.OOBControlMessage;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.stream.ISeekableProvider;
import org.red5.server.stream.IStreamTypeAwareProvider;
import org.red5.server.stream.PlaybackContext;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VodPlayStrategy implements PlayStrategy {

  private static final Logger log = LoggerFactory.getLogger(VodPlayStrategy.class);

  private final PlaybackContext context;

  private final IConsumer source;

  public VodPlayStrategy(PlaybackContext context, IConsumer source) {
    this.context = context;
    this.source = source;
  }

  @Override
  public RTMPMessage prepare(IPlayItem item) throws IOException {
    context.subscriberStream.setState(StreamState.PLAYING);
    sendVodInitControlMessage(item);

    int requestedStart = (int) item.getStart();
    if (requestedStart > 0) {
      int actualPosition = seekToPosition(requestedStart);
      context.streamOffset = (actualPosition == -1) ? requestedStart : actualPosition;
    }

    IMessageInput messageIn = context.messageInput.get();
    return pullFirstMessage(messageIn, item);
  }

  @Override
  public int adjustTimestamp(IRTMPEvent event, int originalTimestamp) {
    if (originalTimestamp > 0 && context.streamStartTimestamp.compareAndSet(-1, originalTimestamp)) {
      log.debug("VodPlayStrategy: first packet ts={}, normalized to 0", originalTimestamp);
      return 0;
    }

    int startTs = context.streamStartTimestamp.get();
    if (startTs > 0 && originalTimestamp > 0) {
      return originalTimestamp - startTs;
    }

    return originalTimestamp;
  }

  @Override
  public boolean hasReachedEnd(IPlayItem item, int eventTimestamp) {
    long requestedLength = item.getLength();
    if (requestedLength < 0) {
      return false;
    }

    int startTs = context.streamStartTimestamp.get();
    if (startTs == -1) {
      return false;
    }

    int playedMilliseconds = eventTimestamp - startTs;
    return (playedMilliseconds - context.streamOffset) >= requestedLength;
  }

  public int seekToPosition(int positionMs) {
    IMessageInput messageIn = context.messageInput.get();
    if (messageIn == null) {
      log.warn("VodPlayStrategy: seek impossible, no input available");
      return -1;
    }

    OOBControlMessage seekMessage = new OOBControlMessage();
    seekMessage.setTarget(ISeekableProvider.KEY);
    seekMessage.setServiceName("seek");

    Map<String, Object> params = new HashMap<>();
    params.put("position", positionMs);
    seekMessage.setServiceParamMap(params);

    messageIn.sendOOBControlMessage(source, seekMessage);

    Object result = seekMessage.getResult();
    if (result instanceof Integer) {
      return (Integer) result;
    }
    return -1;
  }

  public boolean hasVideo() {
    IMessageInput messageIn = context.messageInput.get();
    if (messageIn == null) {
      return false;
    }

    OOBControlMessage checkMessage = new OOBControlMessage();
    checkMessage.setTarget(IStreamTypeAwareProvider.KEY);
    checkMessage.setServiceName("hasVideo");

    messageIn.sendOOBControlMessage(source, checkMessage);

    Object result = checkMessage.getResult();
    if (result instanceof Boolean) {
      return (Boolean) result;
    }
    return false;
  }

  private void sendVodInitControlMessage(IPlayItem item) {
    IMessageInput messageIn = context.messageInput.get();
    if (messageIn == null) {
      log.warn("VodPlayStrategy: init impossible, no input available");
      return;
    }

    OOBControlMessage initMessage = new OOBControlMessage();
    initMessage.setTarget(org.red5.server.messaging.IPassive.KEY);
    initMessage.setServiceName("init");

    Map<String, Object> params = new HashMap<>();
    params.put("startTS", (int) item.getStart());
    initMessage.setServiceParamMap(params);

    messageIn.sendOOBControlMessage(source, initMessage);
  }

  private RTMPMessage pullFirstMessage(IMessageInput messageIn, IPlayItem item) throws IOException {
    if (messageIn == null) {
      throw new IOException("Cannot read first VOD message: no input");
    }

    RTMPMessage message = (RTMPMessage) messageIn.pullMessage();

    if (message != null && item.getLength() == 0) {
      message = pullFirstVideoFrame(messageIn, message);
    }

    if (message != null && message.getBody() != null) {
      int currentTimestamp = message.getBody().getTimestamp();
      message.getBody().setTimestamp(currentTimestamp + context.timestampOffset);
    }

    return message;
  }

  private RTMPMessage pullFirstVideoFrame(IMessageInput messageIn, RTMPMessage currentMessage) {
    while (currentMessage != null && !(currentMessage.getBody() instanceof VideoData)) {
      try {
        Object next = messageIn.pullMessage();
        if (next instanceof RTMPMessage) {
          currentMessage = (RTMPMessage) next;
        } else {
          break;
        }
      } catch (Exception err) {
        log.warn("VodPlayStrategy: error reading zero-length item", err);
        break;
      }
    }
    return currentMessage;
  }

}
