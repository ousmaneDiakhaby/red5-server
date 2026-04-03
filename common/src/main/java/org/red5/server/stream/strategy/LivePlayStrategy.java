package org.red5.server.stream.strategy;

import java.io.IOException;

import org.apache.mina.core.buffer.IoBuffer;
import org.red5.codec.IAudioStreamCodec;
import org.red5.codec.IStreamCodecInfo;
import org.red5.codec.IVideoStreamCodec;
import org.red5.codec.StreamCodecInfo;
import org.red5.server.api.scope.IBroadcastScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.StreamState;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.Notify;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.stream.PlaybackContext;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LivePlayStrategy implements PlayStrategy {

  private static final Logger log = LoggerFactory.getLogger(LivePlayStrategy.class);

  private final PlaybackContext context;

  private final MessageSender sender;

  private int videoBaseTimestamp = -1;

  private int audioBaseTimestamp = -1;

  private int publisherTimestampAtJoin = 0;

  public LivePlayStrategy(PlaybackContext context, MessageSender sender) {
    this.context = context;
    this.sender = sender;
  }

  @Override
  public RTMPMessage prepare(IPlayItem item) throws IOException {
    context.subscriberStream.setState(StreamState.PLAYING);

    context.streamStartTimestamp.set(0);
    videoBaseTimestamp = -1;
    audioBaseTimestamp = -1;
    publisherTimestampAtJoin = 0;

    IMessageInput messageIn = context.messageInput.get();
    if (messageIn == null) {
      throw new IOException("Cannot start live playback: no input available");
    }

    IBroadcastStream liveStream = getLiveBroadcastStream(messageIn);
    if (liveStream == null) {
      log.debug("LivePlayStrategy: live stream not available (publisher not started yet)");
      context.configsDone = true;
      return null;
    }

    sendMetadata(liveStream);
    sendCodecConfigsAndKeyframe(liveStream);
    context.configsDone = true;
    return null;
  }

  @Override
  public int adjustTimestamp(IRTMPEvent event, int originalTimestamp) {
    // ts=0 (config décodeur) et ts=1 (image-clé initiale) sont déjà corrects
    if (originalTimestamp <= 1) {
      return originalTimestamp;
    }

    int startTs = context.streamStartTimestamp.get();

    if (startTs == -1) {
      // Cas limite : messages arrivés avant prepare()
      context.streamStartTimestamp.compareAndSet(-1, originalTimestamp);
      return originalTimestamp;
    }

    if (startTs == 0) {
      return adjustFirstLiveFrameTimestamp(event, originalTimestamp);
    }

    return adjustSubsequentLiveFrameTimestamp(event, originalTimestamp);
  }

  @Override
  public boolean hasReachedEnd(IPlayItem item, int eventTimestamp) {
    return false;
  }

  private IBroadcastStream getLiveBroadcastStream(IMessageInput messageIn) {
    if (messageIn instanceof IBroadcastScope) {
      IBroadcastScope scope = (IBroadcastScope) messageIn;
      return (IBroadcastStream) scope.getClientBroadcastStream();
    }
    return null;
  }

  private void sendMetadata(IBroadcastStream liveStream) {
    Notify metadata = liveStream.getMetaData();
    if (metadata != null) {
      log.debug("LivePlayStrategy: sending metadata");
      sender.sendMessage(RTMPMessage.build(metadata, metadata.getTimestamp()));
    }
  }

  private void sendCodecConfigsAndKeyframe(IBroadcastStream liveStream) {
    IStreamCodecInfo codecInfo = liveStream.getCodecInfo();
    if (!(codecInfo instanceof StreamCodecInfo)) {
      log.debug("LivePlayStrategy: codec info not available");
      return;
    }

    StreamCodecInfo info = (StreamCodecInfo) codecInfo;
    sendVideoDecoderConfig(info.getVideoCodec(), 0);
    sendAudioDecoderConfig(info.getAudioCodec(), 0);
    sendKeyframes(info.getVideoCodec());
  }

  private void sendVideoDecoderConfig(IVideoStreamCodec videoCodec, int timestamp) {
    if (videoCodec == null) {
      return;
    }
    IoBuffer config = videoCodec.getDecoderConfiguration();
    if (config != null) {
      log.debug("LivePlayStrategy: sending video decoder config codec={}", videoCodec.getName());
      sender.sendMessage(RTMPMessage.build(new VideoData(config, true), timestamp));
    }
  }

  private void sendAudioDecoderConfig(IAudioStreamCodec audioCodec, int timestamp) {
    if (audioCodec == null) {
      return;
    }
    IoBuffer config = audioCodec.getDecoderConfiguration();
    if (config != null) {
      log.debug("LivePlayStrategy: sending audio decoder config codec={}", audioCodec.getName());
      sender.sendMessage(RTMPMessage.build(new AudioData(config.asReadOnlyBuffer()), timestamp));
    }
  }

  private void sendKeyframes(IVideoStreamCodec videoCodec) {
    if (videoCodec == null) {
      return;
    }
    IVideoStreamCodec.FrameData[] keyframes = videoCodec.getKeyframes();
    if (keyframes != null && keyframes.length > 0) {
      for (IVideoStreamCodec.FrameData keyframe : keyframes) {
        log.debug("LivePlayStrategy: sending buffered keyframe");
        sender.sendMessage(RTMPMessage.build(new VideoData(keyframe.getFrame(), true), 1));
      }
      return;
    }
    IoBuffer singleKeyframe = videoCodec.getKeyframe();
    if (singleKeyframe != null) {
      log.debug("LivePlayStrategy: sending single keyframe");
      sender.sendMessage(RTMPMessage.build(new VideoData(singleKeyframe, true), 1));
    }
  }

  private int adjustFirstLiveFrameTimestamp(IRTMPEvent event, int originalTimestamp) {
    if (event instanceof VideoData) {
      if (videoBaseTimestamp == -1) {
        videoBaseTimestamp = originalTimestamp;
        publisherTimestampAtJoin = originalTimestamp;
        context.streamStartTimestamp.compareAndSet(0, originalTimestamp);
        return 2;
      }
      return originalTimestamp - videoBaseTimestamp + 2;
    }

    if (event instanceof AudioData) {
      if (audioBaseTimestamp == -1) {
        audioBaseTimestamp = originalTimestamp;
        if (context.streamStartTimestamp.compareAndSet(0, originalTimestamp)) {
          publisherTimestampAtJoin = originalTimestamp;
        }
        return 2;
      }
      return originalTimestamp - audioBaseTimestamp + 2;
    }

    return originalTimestamp;
  }

  private int adjustSubsequentLiveFrameTimestamp(IRTMPEvent event, int originalTimestamp) {
    if (event instanceof VideoData) {
      if (videoBaseTimestamp == -1) {
        videoBaseTimestamp = originalTimestamp;
        return 2;
      }
      return originalTimestamp - videoBaseTimestamp + 2;
    }

    if (event instanceof AudioData) {
      if (audioBaseTimestamp == -1) {
        audioBaseTimestamp = originalTimestamp;
        return 2;
      }
      return originalTimestamp - audioBaseTimestamp + 2;
    }

    int startTs = context.streamStartTimestamp.get();
    if (publisherTimestampAtJoin > 0) {
      return originalTimestamp - publisherTimestampAtJoin + 2;
    }
    return originalTimestamp - startTs;
  }

}
