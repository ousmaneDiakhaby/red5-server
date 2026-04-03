package org.red5.server.stream.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.ISubscriberStream;
import org.red5.server.api.stream.StreamState;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.stream.PlaybackContext;
import org.red5.server.stream.message.RTMPMessage;

@ExtendWith(MockitoExtension.class)
@DisplayName("LivePlayStrategy")
class LivePlayStrategyTest {

  @Mock
  private ISubscriberStream subscriberStream;

  @Mock
  private IMessageInput mockInput;

  @Mock
  private IMessageOutput mockOutput;

  @Mock
  private MessageSender mockSender;

  @Mock
  private IPlayItem mockItem;

  private PlaybackContext context;

  private LivePlayStrategy strategy;

  @BeforeEach
  void setUp() {
    when(subscriberStream.getStreamId()).thenReturn(1);
    AtomicReference<IMessageInput> inputRef = new AtomicReference<>(mockInput);
    AtomicReference<IMessageOutput> outputRef = new AtomicReference<>(mockOutput);
    context = new PlaybackContext(subscriberStream, inputRef, outputRef);
    strategy = new LivePlayStrategy(context, mockSender);
  }


  @Test
  @DisplayName("prepare sets stream state to PLAYING")
  void prepare_setsStreamStateToPlaying() throws IOException {
    strategy.prepare(mockItem);

    verify(subscriberStream).setState(StreamState.PLAYING);
  }

  @Test
  @DisplayName("prepare sets configsDone to true")
  void prepare_setsConfigsDoneTrue() throws IOException {
    strategy.prepare(mockItem);

    assertTrue(context.configsDone, "configsDone must be true after prepare() so PlayEngine starts forwarding live messages");
  }

  @Test
  @DisplayName("prepare sets streamStartTimestamp to 0 (late-subscriber signal)")
  void prepare_setsStreamStartTimestampToZero() throws IOException {
    strategy.prepare(mockItem);

    assertEquals(0, context.streamStartTimestamp.get(), "StreamStartTimestamp 0 signals to adjustTimestamp() that this is a late subscriber");
  }

  @Test
  @DisplayName("prepare returns null (strategy sends its messages internally)")
  void prepare_returnsNull() throws IOException {
    RTMPMessage result = strategy.prepare(mockItem);

    assertNull(result, "LivePlayStrategy sends its initial messages via MessageSender, not by returning them");
  }

  @Test
  @DisplayName("prepare does not send any messages when input is not a broadcast scope")
  void prepare_doesNotSendMessagesWhenInputIsNotBroadcastScope() throws IOException {
    strategy.prepare(mockItem);

    verify(mockSender, never()).sendMessage(any(RTMPMessage.class));
  }

  @Test
  @DisplayName("prepare throws IOException when message input is null")
  void prepare_throwsIOExceptionWhenInputIsNull() {
    context.messageInput.set(null);

    org.junit.jupiter.api.Assertions.assertThrows(IOException.class, () -> strategy.prepare(mockItem), "Should throw IOException when there is no message input to connect to");
  }

  @Test
  @DisplayName("adjustTimestamp returns 0 unchanged (decoder config)")
  void adjustTimestamp_returnsZeroUnchanged() {
    int result = strategy.adjustTimestamp(mock(IRTMPEvent.class), 0);
    assertEquals(0, result, "Timestamp 0 is used for decoder configs and must not be adjusted");
  }

  @Test
  @DisplayName("adjustTimestamp returns 1 unchanged (initial keyframe)")
  void adjustTimestamp_returnsOneUnchanged() {
    int result = strategy.adjustTimestamp(mock(IRTMPEvent.class), 1);
    assertEquals(1, result, "Timestamp 1 is used for the initial keyframe and must not be adjusted");
  }

  @Test
  @DisplayName("adjustTimestamp maps first video frame to timestamp 2")
  void adjustTimestamp_firstVideoFrameReturnsTwoAsTimestamp() {
    context.streamStartTimestamp.set(0);
    VideoData videoEvent = new VideoData();

    int result = strategy.adjustTimestamp(videoEvent, 30000);

    assertEquals(2, result, "The first live video frame should start at timestamp 2 (after decoder config at 0 and keyframe at 1)");
  }

  @Test
  @DisplayName("adjustTimestamp maps first audio frame to timestamp 2")
  void adjustTimestamp_firstAudioFrameReturnsTwoAsTimestamp() {
    context.streamStartTimestamp.set(0);
    AudioData audioEvent = new AudioData();

    int result = strategy.adjustTimestamp(audioEvent, 30000);

    assertEquals(2, result, "The first live audio frame should start at timestamp 2, same as video");
  }

  @Test
  @DisplayName("adjustTimestamp adjusts subsequent video frames relative to the first")
  void adjustTimestamp_subsequentVideoFramesAreRelativeToFirstFrame() {
    context.streamStartTimestamp.set(0);
    VideoData videoEvent = new VideoData();

    strategy.adjustTimestamp(videoEvent, 30000);

    int result = strategy.adjustTimestamp(videoEvent, 30033);

    assertEquals(35, result, "Subsequent video frames should be offset relative to the first frame's publisher timestamp");
  }

  @Test
  @DisplayName("adjustTimestamp ensures audio and video start at same relative timestamp")
  void adjustTimestamp_audioAndVideoStartAtSameTimestamp() {
    context.streamStartTimestamp.set(0);
    VideoData videoEvent = new VideoData();
    AudioData audioEvent = new AudioData();

    int videoTs = strategy.adjustTimestamp(videoEvent, 30000);
    int audioTs = strategy.adjustTimestamp(audioEvent, 30000);

    assertEquals(2, videoTs, "Video should start at timestamp 2");
    assertEquals(2, audioTs, "Audio should also start at timestamp 2 for proper A/V sync");
  }

  @Test
  @DisplayName("adjustTimestamp handles audio arriving before video (still synced)")
  void adjustTimestamp_audioFirstThenVideoAreStillSynced() {
    context.streamStartTimestamp.set(0);
    AudioData audioEvent = new AudioData();
    VideoData videoEvent = new VideoData();

    int audioTs = strategy.adjustTimestamp(audioEvent, 30000);
    int videoTs = strategy.adjustTimestamp(videoEvent, 30000);

    assertEquals(2, audioTs);
    assertEquals(2, videoTs, "Video starting after audio should still be at timestamp 2");
  }

  @Test
  @DisplayName("adjustTimestamp handles edge case when streamStartTimestamp is -1")
  void adjustTimestamp_edgeCase_streamStartTimestampMinusOne() {
    context.streamStartTimestamp.set(-1);
    VideoData videoEvent = new VideoData();

    int result = strategy.adjustTimestamp(videoEvent, 30000);

    assertEquals(30000, result, "When streamStartTimestamp is -1 (before prepare() was called), use the raw timestamp");
  }

  @Test
  @DisplayName("hasReachedEnd always returns false for live streams")
  void hasReachedEnd_alwaysReturnsFalse() {
    boolean result = strategy.hasReachedEnd(mockItem, 999999);

    assertFalse(result, "Live streams never auto-stop based on duration - they stop when the publisher stops");
  }

  @Test
  @DisplayName("hasReachedEnd returns false even at very large timestamps")
  void hasReachedEnd_returnsFalseEvenForVeryLargeTimestamps() {
    boolean result = strategy.hasReachedEnd(mockItem, Integer.MAX_VALUE);

    assertFalse(result);
  }

}
