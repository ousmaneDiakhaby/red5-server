package org.red5.server.stream.strategy;

import java.io.IOException;

import org.red5.server.api.stream.IPlayItem;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.stream.message.RTMPMessage;

public interface PlayStrategy {

  RTMPMessage prepare(IPlayItem item) throws IOException;

  int adjustTimestamp(IRTMPEvent event, int originalTimestamp);

  boolean hasReachedEnd(IPlayItem item, int eventTimestamp);

}
