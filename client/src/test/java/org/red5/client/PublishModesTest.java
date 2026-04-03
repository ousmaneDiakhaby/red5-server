package org.red5.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PublishModesTest {

  @Test
  void liveValueIsLive() {
    assertEquals("live", PublishModes.LIVE);
  }

  @Test
  void recordValueIsRecord() {
    assertEquals("record", PublishModes.RECORD);
  }

  @Test
  void appendValueIsAppend() {
    assertEquals("append", PublishModes.APPEND);
  }

  @Test
  void appendWithGapValueIsAppendWithGap() {
    assertEquals("appendWithGap", PublishModes.APPENDWITHGAP);
  }

  @Test
  void allModesAreDistinct() {
    assertNotEquals(PublishModes.LIVE, PublishModes.RECORD);
    assertNotEquals(PublishModes.LIVE, PublishModes.APPEND);
    assertNotEquals(PublishModes.LIVE, PublishModes.APPENDWITHGAP);
    assertNotEquals(PublishModes.RECORD, PublishModes.APPEND);
    assertNotEquals(PublishModes.RECORD, PublishModes.APPENDWITHGAP);
    assertNotEquals(PublishModes.APPEND, PublishModes.APPENDWITHGAP);
  }
}
