package org.red5.server.session;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SessionTest {

  @Test
  void newSessionIsActiveByDefault() {
    Session session = new Session("abc");
    assertTrue(session.isActive());
  }

  @Test
  void getSessionIdReturnsProvidedId() {
    Session session = new Session("session-42");
    assertEquals("session-42", session.getSessionId());
  }

  @Test
  void noArgConstructorReturnsActiveSession() {
    Session session = new Session();
    assertTrue(session.isActive());
    assertNull(session.getSessionId());
  }

  @Test
  void endDeactivatesSession() {
    Session session = new Session("s1");
    session.end();
    assertFalse(session.isActive());
  }

  @Test
  void getCreatedIsApproximatelyNow() {
    long before = System.currentTimeMillis();
    Session session = new Session("s");
    long after = System.currentTimeMillis();
    assertTrue(session.getCreated() >= before);
    assertTrue(session.getCreated() <= after);
  }

  @Test
  void setAndGetClientId() {
    Session session = new Session("s");
    session.setClientId("client-99");
    assertEquals("client-99", session.getClientId());
  }

  @Test
  void resetClearsClientId() {
    Session session = new Session("s");
    session.setClientId("client-99");
    session.reset();
    assertNull(session.getClientId());
  }

  @Test
  void setAndGetDestinationDirectory() {
    Session session = new Session("s");
    session.setDestinationDirectory("/tmp/uploads");
    assertEquals("/tmp/uploads", session.getDestinationDirectory());
  }

  @Test
  void twoSessionsWithSameIdAreEqual() {
    Session s1 = new Session("same-id");
    Session s2 = new Session("same-id");
    assertEquals(s1, s2);
    assertEquals(s1.hashCode(), s2.hashCode());
  }

  @Test
  void twoSessionsWithDistinctIdsAreNotEqual() {
    Session s1 = new Session("id-1");
    Session s2 = new Session("id-2");
    assertNotEquals(s1, s2);
  }

  @Test
  void sessionEqualsItself() {
    Session session = new Session("id");
    assertEquals(session, session);
  }

  @Test
  void sessionNotEqualToNull() {
    Session session = new Session("id");
    assertNotEquals(null, session);
  }
}
