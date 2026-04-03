package org.red5.server.exception;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ExceptionsTest {

  @Test
  void accessDeniedIsARuntimeException() {
    AccessDeniedException ex = new AccessDeniedException();
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void serviceNotFoundIsARuntimeException() {
    ServiceNotFoundException ex = new ServiceNotFoundException();
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void sharedObjectIsARuntimeException() {
    SharedObjectException ex = new SharedObjectException();
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void streamControlIsARuntimeException() {
    StreamControlException ex = new StreamControlException();
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void streamDataIsARuntimeException() {
    StreamDataException ex = new StreamDataException();
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void scopeHandlerNotFoundContainsHandlerName() {
    ScopeHandlerNotFoundException ex = new ScopeHandlerNotFoundException("monHandler");
    assertInstanceOf(RuntimeException.class, ex);
    assertTrue(ex.getMessage().contains("monHandler"));
  }

  @Test
  void scopeHandlerNotFoundWithEmptyHandler() {
    ScopeHandlerNotFoundException ex = new ScopeHandlerNotFoundException("");
    assertNotNull(ex.getMessage());
  }
}
