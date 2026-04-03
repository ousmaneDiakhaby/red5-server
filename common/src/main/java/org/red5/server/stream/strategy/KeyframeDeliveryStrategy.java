package org.red5.server.stream.strategy;

import org.red5.codec.IVideoStreamCodec;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.stream.IFrameDropper;
import org.red5.server.stream.VideoFrameDropper;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyframeDeliveryStrategy {

  private static final Logger log = LoggerFactory.getLogger(KeyframeDeliveryStrategy.class);

  private final IFrameDropper videoFrameDropper = new VideoFrameDropper();

  private boolean waitingForKeyframe = false;

  private int bufferedInterframeIndex = -1;

  public void onPlayStart() {
    videoFrameDropper.reset(IFrameDropper.SEND_KEYFRAMES_CHECK);
    waitingForKeyframe = true;
    log.debug("KeyframeDeliveryStrategy: waiting for first keyframe");
  }

  public void onKeyframeAlreadyAvailable() {
    bufferedInterframeIndex = 0;
    videoFrameDropper.reset(IFrameDropper.SEND_ALL);
    waitingForKeyframe = false;
    log.debug("KeyframeDeliveryStrategy: keyframe available, switching to SEND_ALL");
  }

  /** Appelée à la reprise après pause : on attend une nouvelle image-clé. */
  public void onResume() {
    videoFrameDropper.reset(IFrameDropper.SEND_KEYFRAMES_CHECK);
    log.debug("KeyframeDeliveryStrategy: resuming, switching to SEND_KEYFRAMES_CHECK");
  }

  /**
   * Détermine si la trame vidéo doit être envoyée ou abandonnée.
   *
   * @param message              la trame vidéo entrante
   * @param pendingVideoCount    nombre de messages vidéo déjà en attente
   * @param maxPendingFrames     seuil au-delà duquel on écrête
   * @param maxSequentialPending seuil de trames consécutives en attente
   * @param currentSequentialCount nombre courant de trames consécutives en attente
   * @return true si la trame doit être envoyée
   */

  public boolean shouldSendVideoFrame(RTMPMessage message, long pendingVideoCount, int maxPendingFrames, int maxSequentialPending, int currentSequentialCount) {
    VideoData videoData = (VideoData) message.getBody();

    if (waitingForKeyframe && videoData.isKeyFrame()) {
      log.debug("KeyframeDeliveryStrategy: first keyframe received, switching to SEND_INTERFRAMES");
      videoFrameDropper.reset(IFrameDropper.SEND_INTERFRAMES);
      waitingForKeyframe = false;
    }

    if (!videoFrameDropper.canSendPacket(message, pendingVideoCount)) {
      return false;
    }

    if (pendingVideoCount > maxPendingFrames || currentSequentialCount > maxSequentialPending) {
      videoFrameDropper.dropPacket(message);
      return false;
    }

    return true;
  }

  public void recordDroppedFrame(RTMPMessage message) {
    videoFrameDropper.dropPacket(message);
  }

  public boolean hasPendingBufferedFrames() {
    return bufferedInterframeIndex >= 0;
  }
  public VideoData getNextBufferedInterframe(IVideoStreamCodec videoCodec, int liveFrameTimestamp) {
    IVideoStreamCodec.FrameData frameData = videoCodec.getInterframe(bufferedInterframeIndex);
    if (frameData != null) {
      bufferedInterframeIndex++;
      VideoData interframe = new VideoData(frameData.getFrame());
      interframe.setTimestamp(liveFrameTimestamp);
      return interframe;
    }
    bufferedInterframeIndex = -1;
    return null;
  }
  public boolean isWaitingForKeyframe() {
    return waitingForKeyframe;
  }

}
