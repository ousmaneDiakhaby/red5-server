package org.red5.server.stream.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.red5.codec.IVideoStreamCodec;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.stream.message.RTMPMessage;

@ExtendWith(MockitoExtension.class)
@DisplayName("KeyframeDeliveryStrategy")
class KeyframeDeliveryStrategyTest {

  private KeyframeDeliveryStrategy strategy;

  @BeforeEach
  void setUp() {
    strategy = new KeyframeDeliveryStrategy();
  }

  @Test
  @DisplayName("initially not waiting for keyframe")
  void initialState_notWaitingForKeyframe() {
    assertFalse(strategy.isWaitingForKeyframe(), "Should not be waiting for keyframe before any setup call");
  }

  @Test
  @DisplayName("initially no pending buffered frames")
  void initialState_noPendingBufferedFrames() {
    assertFalse(strategy.hasPendingBufferedFrames(), "Should not have buffered frames before any setup call");
  }


  @Test
  @DisplayName("onPlayStart sets waiting-for-keyframe flag")
  void onPlayStart_setsWaitingForKeyframe() {
    strategy.onPlayStart();

    assertTrue(strategy.isWaitingForKeyframe(), "After onPlayStart(), the strategy should be waiting for the first keyframe");
  }

  @Test
  @DisplayName("onPlayStart does not set buffered frames flag")
  void onPlayStart_doesNotSetBufferedFrames() {
    strategy.onPlayStart();

    assertFalse(strategy.hasPendingBufferedFrames(), "After onPlayStart(), there should be no buffered frames (keyframe not received yet)");
  }

  @Test
  @DisplayName("onKeyframeAlreadyAvailable clears waiting flag")
  void onKeyframeAlreadyAvailable_clearsWaitingFlag() {
    strategy.onPlayStart(); // first put in waiting mode
    strategy.onKeyframeAlreadyAvailable();

    assertFalse(strategy.isWaitingForKeyframe(), "After onKeyframeAlreadyAvailable(), we should not be waiting for a keyframe anymore");
  }

  @Test
  @DisplayName("onKeyframeAlreadyAvailable sets buffered frames flag")
  void onKeyframeAlreadyAvailable_setsPendingBufferedFrames() {
    strategy.onKeyframeAlreadyAvailable();

    assertTrue(strategy.hasPendingBufferedFrames(), "After onKeyframeAlreadyAvailable(), the strategy should know there are buffered frames to send");
  }

  @Test
  @DisplayName("onResume does not set waiting-for-keyframe flag (just resets dropper)")
  void onResume_doesNotSetWaitingFlag() {
    strategy.onResume();

    assertFalse(strategy.isWaitingForKeyframe(), "onResume() should not set the waiting-for-keyframe flag");
  }


  @Test
  @DisplayName("getNextBufferedInterframe returns frame when codec has interframes")
  void getNextBufferedInterframe_returnsFrameWhenAvailable() {
    strategy.onKeyframeAlreadyAvailable();

    IVideoStreamCodec codec = mock(IVideoStreamCodec.class);
    IVideoStreamCodec.FrameData frameData = mock(IVideoStreamCodec.FrameData.class);
    org.apache.mina.core.buffer.IoBuffer frameBuffer = org.apache.mina.core.buffer.IoBuffer.allocate(10);
    when(frameData.getFrame()).thenReturn(frameBuffer);
    when(codec.getInterframe(0)).thenReturn(frameData);

    VideoData result = strategy.getNextBufferedInterframe(codec, 1000);

    assertNotNull(result, "Should return a VideoData when the codec has an interframe at index 0");
    assertEquals(1000, result.getTimestamp(), "The returned frame should have the live frame's timestamp");
  }

  @Test
  @DisplayName("getNextBufferedInterframe returns null and resets index when no more frames")
  void getNextBufferedInterframe_returnsNullAndResetsWhenNoMoreFrames() {
    strategy.onKeyframeAlreadyAvailable(); // sets bufferedInterframeIndex = 0

    IVideoStreamCodec codec = mock(IVideoStreamCodec.class);
    when(codec.getInterframe(0)).thenReturn(null);

    VideoData result = strategy.getNextBufferedInterframe(codec, 1000);

    assertNull(result, "Should return null when the codec has no more interframes");
    assertFalse(strategy.hasPendingBufferedFrames(), "After returning null, the buffered frames flag should be reset to false");
  }

  @Test
  @DisplayName("getNextBufferedInterframe advances the frame index on each call")
  void getNextBufferedInterframe_advancesIndexOnEachCall() {
    strategy.onKeyframeAlreadyAvailable(); // sets bufferedInterframeIndex = 0

    IVideoStreamCodec codec = mock(IVideoStreamCodec.class);
    IVideoStreamCodec.FrameData frame0 = mock(IVideoStreamCodec.FrameData.class);
    IVideoStreamCodec.FrameData frame1 = mock(IVideoStreamCodec.FrameData.class);
    when(frame0.getFrame()).thenReturn(org.apache.mina.core.buffer.IoBuffer.allocate(4));
    when(frame1.getFrame()).thenReturn(org.apache.mina.core.buffer.IoBuffer.allocate(4));
    when(codec.getInterframe(0)).thenReturn(frame0);
    when(codec.getInterframe(1)).thenReturn(frame1);

    VideoData first = strategy.getNextBufferedInterframe(codec, 1000);
    VideoData second = strategy.getNextBufferedInterframe(codec, 1033);

    assertNotNull(first, "First call should return a frame");
    assertNotNull(second, "Second call should return a different frame");
  }

  @Test
  @DisplayName("shouldSendVideoFrame drops frame when too many are pending")
  void shouldSendVideoFrame_dropsWhenPendingAboveThreshold() {
    // Not waiting for keyframe, so the dropper is in default state
    VideoData videoData = new VideoData();
    RTMPMessage message = RTMPMessage.build(videoData);

    long pendingVideos = 15; // above maxPendingFrames of 10
    int maxPending = 10;
    int maxSequential = 10;
    int currentSequential = 0;

    boolean shouldSend = strategy.shouldSendVideoFrame(message, pendingVideos, maxPending, maxSequential, currentSequential);

    assertFalse(shouldSend, "Should drop the frame when pending video count exceeds the threshold");
  }

  @Test
  @DisplayName("shouldSendVideoFrame drops frame when sequential pending count is too high")
  void shouldSendVideoFrame_dropsWhenSequentialPendingTooHigh() {
    VideoData videoData = new VideoData();
    RTMPMessage message = RTMPMessage.build(videoData);

    long pendingVideos = 2; // below maxPendingFrames
    int maxPending = 10;
    int maxSequential = 10;
    int currentSequential = 15; // above maxSequential of 10

    boolean shouldSend = strategy.shouldSendVideoFrame(message, pendingVideos, maxPending, maxSequential, currentSequential);

    assertFalse(shouldSend, "Should drop the frame when sequential pending count exceeds the threshold");
  }

}
