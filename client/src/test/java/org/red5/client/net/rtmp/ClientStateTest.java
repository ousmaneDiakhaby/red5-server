package org.red5.client.net.rtmp;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ClientStateTest {

  @Test
  void allStatesArePresent() {
    ClientState[] states = ClientState.values();
    assertNotNull(states);
    assertEquals(9, states.length);
  }

  @Test
  void mainStatesAreAccessible() {
    assertNotNull(ClientState.UNINIT);
    assertNotNull(ClientState.CONNECTING);
    assertNotNull(ClientState.CONNECTED);
    assertNotNull(ClientState.PUBLISHING);
    assertNotNull(ClientState.PLAYING);
    assertNotNull(ClientState.DISCONNECTED);
  }

  @Test
  void valueOfWorksWithExactName() {
    assertEquals(ClientState.CONNECTED, ClientState.valueOf("CONNECTED"));
    assertEquals(ClientState.DISCONNECTED, ClientState.valueOf("DISCONNECTED"));
  }

  @Test
  void stateOrderIsConsistent() {
    assertTrue(ClientState.UNINIT.ordinal() < ClientState.CONNECTING.ordinal());
    assertTrue(ClientState.CONNECTING.ordinal() < ClientState.CONNECTED.ordinal());
  }
}
