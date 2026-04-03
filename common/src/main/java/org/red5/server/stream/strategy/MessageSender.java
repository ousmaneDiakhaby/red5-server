package org.red5.server.stream.strategy;

import org.red5.server.stream.message.RTMPMessage;

public interface MessageSender {

  void sendMessage(RTMPMessage message);

}
