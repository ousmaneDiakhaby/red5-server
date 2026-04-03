package org.red5.proxy;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ClientTypeTest {

  @Test
  void fourProtocolsArePresent() {
    assertEquals(4, ClientType.values().length);
  }

  @Test
  void eachTypeIsAccessible() {
    assertNotNull(ClientType.RTMP);
    assertNotNull(ClientType.RTMPS);
    assertNotNull(ClientType.RTMPT);
    assertNotNull(ClientType.RTMPE);
  }

  @Test
  void valueOfWorksWithExactName() {
    assertEquals(ClientType.RTMP, ClientType.valueOf("RTMP"));
    assertEquals(ClientType.RTMPS, ClientType.valueOf("RTMPS"));
  }

  @Test
  void typesAreDistinct() {
    assertNotEquals(ClientType.RTMP, ClientType.RTMPS);
    assertNotEquals(ClientType.RTMP, ClientType.RTMPT);
    assertNotEquals(ClientType.RTMP, ClientType.RTMPE);
  }
}
