package org.red5.server.stream.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
import org.red5.server.messaging.IConsumer;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.messaging.OOBControlMessage;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.stream.PlaybackContext;
import org.red5.server.stream.message.RTMPMessage;

@ExtendWith(MockitoExtension.class)
@DisplayName("VodPlayStrategy")
class VodPlayStrategyTest {

  @Mock
  private ISubscriberStream subscriberStream;

  @Mock
  private IMessageInput mockInput;

  @Mock
  private IMessageOutput mockOutput;

  @Mock
  private IConsumer mockSource;

  @Mock
  private IPlayItem mockItem;

  private PlaybackContext context;

  private VodPlayStrategy strategy;

  @BeforeEach
  void setUp() {
    when(subscriberStream.getStreamId()).thenReturn(1);
    AtomicReference<IMessageInput> inputRef = new AtomicReference<>(mockInput);
    AtomicReference<IMessageOutput> outputRef = new AtomicReference<>(mockOutput);
    context = new PlaybackContext(subscriberStream, inputRef, outputRef);
    strategy = new VodPlayStrategy(context, mockSource);
  }

  @Test
  @DisplayName("adjustTimestamp normalizes first non-zero timestamp to 0")
  void adjustTimestamp_normalizesFirstTimestampToZero() {
    IRTMPEvent event = mock(IRTMPEvent.class);

    int result = strategy.adjustTimestamp(event, 5000);

    assertEquals(0, result, "The first packet in a VOD file should always be normalized to timestamp 0");
  }

  @Test
  @DisplayName("adjustTimestamp records the first timestamp as the stream start reference")
  void adjustTimestamp_recordsFirstTimestampAsStartReference() {
    IRTMPEvent event = mock(IRTMPEvent.class);

    strategy.adjustTimestamp(event, 5000);

    assertEquals(5000, context.streamStartTimestamp.get(), "The first packet's timestamp should be stored as the start reference for future normalization");
  }

  @Test
  @DisplayName("adjustTimestamp adjusts subsequent timestamps relative to the first")
  void adjustTimestamp_adjustsSubsequentTimestampsRelatively() {
    IRTMPEvent event = mock(IRTMPEvent.class);

    strategy.adjustTimestamp(event, 5000);
    int result = strategy.adjustTimestamp(event, 5500);

    assertEquals(500, result, "500ms after the start reference should map to output timestamp 500");
  }

  @Test
  @DisplayName("adjustTimestamp does not adjust timestamp 0 (keeps it as 0)")
  void adjustTimestamp_doesNotAdjustTimestampZero() {
    IRTMPEvent event = mock(IRTMPEvent.class);

    int result = strategy.adjustTimestamp(event, 0);

    assertEquals(0, result, "A timestamp of 0 should not set the start reference (must be > 0)");
    assertEquals(-1, context.streamStartTimestamp.get(), "The start reference must remain unset when the first timestamp is 0");
  }

  @Test
  @DisplayName("adjustTimestamp handles playlist offset correctly")
  void adjustTimestamp_addsPlaylistOffset() {
    context.timestampOffset = 30000;
    IRTMPEvent event = mock(IRTMPEvent.class);

    strategy.adjustTimestamp(event, 5000);
    int result = strategy.adjustTimestamp(event, 5500);

    assertEquals(500, result, "500ms into the second playlist item should still normalize correctly");
  }

  @Test
  @DisplayName("hasReachedEnd returns false when length is -1 (play whole file)")
  void hasReachedEnd_returnsFalseWhenLengthIsMinusOne() {
    when(mockItem.getLength()).thenReturn(-1L);

    boolean result = strategy.hasReachedEnd(mockItem, Integer.MAX_VALUE);

    assertFalse(result, "Length -1 means 'play the whole file', so we never auto-stop");
  }

  @Test
  @DisplayName("hasReachedEnd returns false before stream has started")
  void hasReachedEnd_returnsFalseBeforeStreamStarted() {
    when(mockItem.getLength()).thenReturn(60000L);

    boolean result = strategy.hasReachedEnd(mockItem, 70000);

    assertFalse(result, "Cannot have reached the end before the first packet is sent (streamStartTimestamp == -1)");
  }

  @Test
  @DisplayName("hasReachedEnd returns true when played past the requested length")
  void hasReachedEnd_returnsTrueWhenPastLength() {
    when(mockItem.getLength()).thenReturn(60000L);
    context.streamStartTimestamp.set(1000);

    boolean result = strategy.hasReachedEnd(mockItem, 62000);

    assertTrue(result, "Should return true when we have played past the requested 60 seconds");
  }

  @Test
  @DisplayName("hasReachedEnd returns false when still within the requested length")
  void hasReachedEnd_returnsFalseWhenStillWithinLength() {
    when(mockItem.getLength()).thenReturn(60000L);
    context.streamStartTimestamp.set(1000);

    boolean result = strategy.hasReachedEnd(mockItem, 31000);

    assertFalse(result, "Should return false when we haven't played the full 60 seconds yet");
  }

  @Test
  @DisplayName("hasReachedEnd accounts for the stream offset when checking duration")
  void hasReachedEnd_accountsForStreamOffset() {
    when(mockItem.getLength()).thenReturn(30000L);
    context.streamStartTimestamp.set(1000);
    context.streamOffset = 10000;

    boolean result = strategy.hasReachedEnd(mockItem, 41000);

    assertTrue(result, "The stream offset (seek start position) should be accounted for in end detection");
  }

  @Test
  @DisplayName("seekToPosition sends an OOB seek message to the file reader")
  void seekToPosition_sendsOOBSeekMessage() {
    doAnswer(invocation -> {
      OOBControlMessage msg = invocation.getArgument(1);
      msg.setResult(4500);
      return null;
    }).when(mockInput).sendOOBControlMessage(any(), any());

    int result = strategy.seekToPosition(5000);

    assertEquals(4500, result, "seekToPosition should return the actual position the file reader seeked to");
    verify(mockInput).sendOOBControlMessage(any(), any());
  }

  @Test
  @DisplayName("seekToPosition returns -1 when there is no message input")
  void seekToPosition_returnsMinusOneWhenNoInput() {
    context.messageInput.set(null);

    int result = strategy.seekToPosition(5000);

    assertEquals(-1, result, "seekToPosition should return -1 gracefully when there is no message input");
  }

  @Test
  @DisplayName("seekToPosition returns -1 when file reader returns a non-Integer result")
  void seekToPosition_returnsMinusOneWhenResultIsNotInteger() {
    doAnswer(invocation -> {
      return null;
    }).when(mockInput).sendOOBControlMessage(any(), any());

    int result = strategy.seekToPosition(5000);

    assertEquals(-1, result, "seekToPosition should return -1 when the file reader does not support seeking");
  }

  @Test
  @DisplayName("hasVideo returns true when file reader says there is video")
  void hasVideo_returnsTrueWhenFileHasVideo() {
    doAnswer(invocation -> {
      OOBControlMessage msg = invocation.getArgument(1);
      msg.setResult(Boolean.TRUE);
      return null;
    }).when(mockInput).sendOOBControlMessage(any(), any());

    boolean result = strategy.hasVideo();

    assertTrue(result, "hasVideo should return true when the file reader confirms there is video");
  }

  @Test
  @DisplayName("hasVideo returns false when file reader says there is no video (audio-only)")
  void hasVideo_returnsFalseForAudioOnlyStream() {
    doAnswer(invocation -> {
      OOBControlMessage msg = invocation.getArgument(1);
      msg.setResult(Boolean.FALSE);
      return null;
    }).when(mockInput).sendOOBControlMessage(any(), any());

    boolean result = strategy.hasVideo();

    assertFalse(result, "hasVideo should return false for audio-only streams");
  }

  @Test
  @DisplayName("hasVideo returns false when there is no message input")
  void hasVideo_returnsFalseWhenNoInput() {
    context.messageInput.set(null);

    boolean result = strategy.hasVideo();

    assertFalse(result, "hasVideo should return false gracefully when there is no input");
  }

  @Test
  @DisplayName("prepare sets stream state to PLAYING")
  void prepare_setsStreamStateToPlaying() throws IOException {
    when(mockItem.getStart()).thenReturn(0L);

    when(mockInput.pullMessage()).thenReturn(null);

    strategy.prepare(mockItem);

    verify(subscriberStream).setState(StreamState.PLAYING);
  }

  @Test
  @DisplayName("prepare returns null when file has no messages")
  void prepare_returnsNullWhenFileIsEmpty() throws IOException {
    when(mockItem.getStart()).thenReturn(0L);
    when(mockInput.pullMessage()).thenReturn(null);

    RTMPMessage result = strategy.prepare(mockItem);

    assertNull(result, "Should return null when the file reader has no messages");
  }

  @Test
  @DisplayName("prepare returns the first message from the file")
  void prepare_returnsFirstMessageFromFile() throws IOException {
    when(mockItem.getStart()).thenReturn(0L);
    when(mockItem.getLength()).thenReturn(-1L);

    AudioData audioData = new AudioData();
    audioData.setTimestamp(1000);
    RTMPMessage firstMessage = RTMPMessage.build(audioData);
    when(mockInput.pullMessage()).thenReturn(firstMessage);

    RTMPMessage result = strategy.prepare(mockItem);

    assertNotNull(result, "Should return the first message from the file");
  }

}
