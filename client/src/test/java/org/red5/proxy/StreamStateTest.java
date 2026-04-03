package org.red5.proxy;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StreamStateTest {

  @Test
  void allStatesArePresent() {
    assertEquals(7, StreamState.values().length);
  }

  @Test
  void mainStatesAreAccessible() {
    assertNotNull(StreamState.UNINITIALIZED);
    assertNotNull(StreamState.STOPPED);
    assertNotNull(StreamState.CONNECTING);
    assertNotNull(StreamState.PUBLISHING);
    assertNotNull(StreamState.PUBLISHED);
    assertNotNull(StreamState.UNPUBLISHED);
  }

  @Test
  void valueOfWorksWithExactName() {
    assertEquals(StreamState.PUBLISHED, StreamState.valueOf("PUBLISHED"));
    assertEquals(StreamState.STOPPED, StreamState.valueOf("STOPPED"));
  }
}
