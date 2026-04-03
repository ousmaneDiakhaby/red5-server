package org.red5.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class Red5ClientTest {

  @Test
  void getVersionReturnsNonNullString() {
    assertNotNull(Red5Client.getVersion());
  }

  @Test
  void getVersionContainsRedClient() {
    assertTrue(Red5Client.getVersion().contains("Red5 Client"));
  }

  @Test
  void versionAndGetVersionAreIdentical() {
    assertEquals(Red5Client.VERSION, Red5Client.getVersion());
  }

  @Test
  void instantiationDoesNotThrowException() {
    assertDoesNotThrow(() -> new Red5Client());
  }
}
