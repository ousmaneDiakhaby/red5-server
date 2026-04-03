/*
 * RED5 Open Source Media Server - https://github.com/Red5/ Copyright 2006-2023 by respective authors (see below). All rights reserved. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
 * required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package org.red5.server.stream;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.mina.core.buffer.IoBuffer;
import org.red5.codec.IVideoStreamCodec;
import org.red5.io.amf.Output;
import org.red5.io.utils.ObjectMap;
import org.red5.server.api.scheduling.IScheduledJob;
import org.red5.server.api.scheduling.ISchedulingService;
import org.red5.server.api.scope.IBroadcastScope;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.IPlaylistSubscriberStream;
import org.red5.server.api.stream.ISubscriberStream;
import org.red5.server.api.stream.OperationNotSupportedException;
import org.red5.server.api.stream.StreamState;
import org.red5.server.api.stream.support.DynamicPlayItem;
import org.red5.server.messaging.AbstractMessage;
import org.red5.server.messaging.IConsumer;
import org.red5.server.messaging.IFilter;
import org.red5.server.messaging.IMessage;
import org.red5.server.messaging.IMessageComponent;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.messaging.IPipe;
import org.red5.server.messaging.IPipeConnectionListener;
import org.red5.server.messaging.IProvider;
import org.red5.server.messaging.IPushableConsumer;
import org.red5.server.messaging.InMemoryPushPushPipe;
import org.red5.server.messaging.OOBControlMessage;
import org.red5.server.messaging.PipeConnectionEvent;
import org.red5.server.net.rtmp.event.Aggregate;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.Notify;
import org.red5.server.net.rtmp.event.Ping;
import org.red5.server.net.rtmp.event.Ping.PingType;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.net.rtmp.message.Constants;
import org.red5.server.net.rtmp.message.Header;
import org.red5.server.net.rtmp.status.Status;
import org.red5.server.net.rtmp.status.StatusCodes;
import org.red5.server.stream.message.RTMPMessage;
import org.red5.server.stream.message.ResetMessage;
import org.red5.server.stream.message.StatusMessage;
import org.red5.server.stream.IStreamData;
import org.red5.server.stream.strategy.KeyframeDeliveryStrategy;
import org.red5.server.stream.strategy.LivePlayStrategy;
import org.red5.server.stream.strategy.MessageSender;
import org.red5.server.stream.strategy.PlayStrategy;
import org.red5.server.stream.strategy.VodPlayStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates playback of a stream item for one subscriber.
 *
 * PlayEngine is responsible for the stream lifecycle (start → play → pause → stop → close)
 * and delegates stream-type-specific behaviour to one of three strategies:
 * - {@link LivePlayStrategy}: sets up a live push stream (sends metadata, codec configs, keyframes)
 * - {@link VodPlayStrategy}: sets up a VOD pull stream (seeks to start position, pulls first message)
 * - {@link KeyframeDeliveryStrategy}: manages frame dropping and keyframe waiting for live streams
 *
 * @author The Red5 Project
 * @author Steven Gong
 * @author Paul Gregoire (mondain@gmail.com)
 * @author Dan Rossi
 * @author Tiago Daniel Jacobs (tiago@imdt.com.br)
 * @author Vladimir Hmelyoff (vlhm@splitmedialabs.com)
 * @author Andy Shaules
 */
public final class PlayEngine implements IFilter, IPushableConsumer, IPipeConnectionListener {

    private static final Logger log = LoggerFactory.getLogger(PlayEngine.class);

    private static boolean isDebug = log.isDebugEnabled();

    private static boolean isTrace = log.isTraceEnabled();

    private static final int PLAY_LIVE = 0;

    private static final int PLAY_VOD = 1;

    private static final int PLAY_WAIT = 2;

    private static final int PLAY_NOT_FOUND = 3;

    private final AtomicReference<IMessageInput> msgInReference = new AtomicReference<>();

    private final AtomicReference<IMessageOutput> msgOutReference = new AtomicReference<>();

    private final ISubscriberStream subscriberStream;

    private ISchedulingService schedulingService;

    private IConsumerService consumerService;

    private IProviderService providerService;

    private Number streamId;

    private PlayStrategy currentStrategy;

    private final KeyframeDeliveryStrategy keyframeStrategy = new KeyframeDeliveryStrategy();

    private final PlaybackContext context;

    /**
     * Receive video?
     */
    private boolean receiveVideo = true;

    /**
     * Receive audio?
     */
    private boolean receiveAudio = true;

    private boolean pullMode;

    private String waitLiveJob;

    private AtomicReference<IPlayItem> currentItem = new AtomicReference<>();

    private RTMPMessage pendingMessage;

    /**
     * Interval in ms to check for buffer underruns in VOD streams.
     */
    private int bufferCheckInterval = 0;

    /**
     * Number of pending messages at which a NetStream.Play.InsufficientBW message is generated for VOD streams.
     */
    private int underrunTrigger = 10;

    /**
     * Threshold for number of pending video frames
     */
    private int maxPendingVideoFrames = 10;

    /**
     * If we have more than 1 pending video frames, but less than maxPendingVideoFrames, continue sending until there are this many
     * sequential frames with more than 1 pending
     */
    private int maxSequentialPendingVideoFrames = 10;

    /**
     * the number of sequential video frames with > 0 pending frames
     */
    private int numSequentialPendingVideoFrames = 0;

    /**
     * Start time of stream playback. It's not a time when the stream is being played but the time when the stream should be played if it's
     * played from the very beginning. Eg. A stream is played at timestamp 5s on 1:00:05. The playbackStart is 1:00:00.
     */
    private volatile long playbackStart;

    /**
     * Timestamp when buffer should be checked for underruns next.
     */
    private long nextCheckBufferUnderrun;

    /**
     * Timestamp of the last message sent to the client.
     */
    private int lastMessageTs = -1;

    /**
     * Number of bytes sent.
     */
    private AtomicLong bytesSent = new AtomicLong(0);

    /**
     * Send blank audio packet next?
     */
    private boolean sendBlankAudio;

    /**
     * Flag denoting whether or not the push and pull job is scheduled. The job makes sure messages are sent to the client.
     */
    private volatile String pullAndPush;

    /**
     * Flag denoting whether or not the job that closes stream after buffer runs out is scheduled.
     */
    private volatile String deferredStop;

    /**
     * Monitor guarding completion of a given push/pull run. Used to wait for job cancellation to finish.
     */
    private final AtomicBoolean pushPullRunning = new AtomicBoolean(false);

    /**
     * List of pending operations
     */
    private ConcurrentLinkedQueue<Runnable> pendingOperations = new ConcurrentLinkedQueue<>();

    // Keep count of dropped packets so we can log every so often.
    private long droppedPacketsCount;

    private long droppedPacketsCountLastLogTimestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

    private long droppedPacketsCountLogInterval = 60 * 1000L;

    private PlayEngine(Builder builder) {
        subscriberStream = builder.subscriberStream;
        schedulingService = builder.schedulingService;
        consumerService = builder.consumerService;
        providerService = builder.providerService;
        streamId = subscriberStream.getStreamId();

        context = new PlaybackContext(subscriberStream, msgInReference, msgOutReference);
    }

    /**
     * Builder pattern
     */
    public final static class Builder {

        private ISubscriberStream subscriberStream;

        private ISchedulingService schedulingService;

        private IConsumerService consumerService;

        private IProviderService providerService;

        public Builder(ISubscriberStream subscriberStream, ISchedulingService schedulingService, IConsumerService consumerService, IProviderService providerService) {
            this.subscriberStream = subscriberStream;
            this.schedulingService = schedulingService;
            this.consumerService = consumerService;
            this.providerService = providerService;
        }

        public PlayEngine build() {
            return new PlayEngine(this);
        }

    }

    public void setBufferCheckInterval(int bufferCheckInterval) {
        this.bufferCheckInterval = bufferCheckInterval;
    }

    public void setUnderrunTrigger(int underrunTrigger) {
        this.underrunTrigger = underrunTrigger;
    }

    public void setMaxPendingVideoFrames(int maxPendingVideoFrames) {
        this.maxPendingVideoFrames = maxPendingVideoFrames;
    }

    public void setMaxSequentialPendingVideoFrames(int maxSequentialPendingVideoFrames) {
        this.maxSequentialPendingVideoFrames = maxSequentialPendingVideoFrames;
    }

    void setMessageOut(IMessageOutput msgOut) {
        this.msgOutReference.set(msgOut);
    }

    /**
     * Initializes the engine. Must be called before play().
     * Changes stream
     */
    public void start() {
        if (isDebug) {
            log.debug("start - subscriber stream state: {}", (subscriberStream != null ? subscriberStream.getState() : null));
        }
        switch (subscriberStream.getState()) {
            case UNINIT:
                subscriberStream.setState(StreamState.STOPPED);
                IMessageOutput out = consumerService.getConsumerOutput(subscriberStream);
                if (msgOutReference.compareAndSet(null, out)) {
                    out.subscribe(this, null);
                } else if (isDebug) {
                    log.debug("Message output was already set for stream: {}", subscriberStream);
                }
                break;
            default:
                throw new IllegalStateException(String.format("Cannot start in current state: %s", subscriberStream.getState()));
        }
    }

    /**
     * Starts playing the given item with a reset (default behavior).
     *
     * @param item the item to play
     */
    public void play(IPlayItem item) throws StreamNotFoundException, IllegalStateException, IOException {
        play(item, true);
    }

    /**
     * Starts playing the given item.
     *
     * This method determines the stream type (live, VOD, wait, not found),
     * creates the appropriate strategy, and starts playback.
     *
     * See: https://www.adobe.com/devnet/adobe-media-server/articles/dynstream_actionscript.html
     *
     * @param item      the item to play
     * @param withReset if true, send a reset notification before starting playback
     */
    public void play(IPlayItem item, boolean withReset) throws StreamNotFoundException, IllegalStateException, IOException {
        // Must be in STOPPED state to play
        switch (subscriberStream.getState()) {
            case STOPPED:
                IMessageInput in = msgInReference.get();
                if (in != null) {
                    in.unsubscribe(this);
                    msgInReference.set(null);
                }
                break;
            default:
                throw new IllegalStateException("Cannot play from non-stopped state");
        }

        // Determine the play type from item.start:
        // -2 = live then VOD, -1 = live only, 0+ = VOD at that position (seconds)
        int type = (int) (item.getStart() / 1000);
        IScope scope = subscriberStream.getScope();
        String itemName = item.getName();
        IProviderService.INPUT_TYPE sourceType = providerService.lookupProviderInput(scope, itemName, type);
        int playDecision = determinePlayDecision(type, sourceType);

        currentItem.set(item);
        long itemLength = item.getLength();
        log.debug("Play decision: {} (0=Live, 1=File, 2=Wait, 3=N/A) item length: {}", playDecision, itemLength);

        // Reset context for new playback
        context.streamStartTimestamp.set(-1);
        context.configsDone = false;

        boolean sendNotifications = true;
        RTMPMessage firstMessage = null;

        switch (playDecision) {
            case PLAY_LIVE:
                IMessageInput liveIn = providerService.getLiveProviderInput(scope, itemName, false);
                if (!msgInReference.compareAndSet(null, liveIn)) {
                    sendStreamNotFoundStatus(item);
                    throw new StreamNotFoundException(itemName);
                }
                sendNotifications = setupLivePlayback(item, liveIn, withReset);
                break;

            case PLAY_WAIT:
                IMessageInput waitIn = providerService.getLiveProviderInput(scope, itemName, true);
                if (msgInReference.compareAndSet(null, waitIn)) {
                    setupWaitForLiveStream(itemName, type, itemLength);
                } else if (isDebug) {
                    log.debug("Message input already set for {}", itemName);
                }
                break;

            case PLAY_VOD:
                IMessageInput vodIn = providerService.getVODProviderInput(scope, itemName);
                if (!msgInReference.compareAndSet(null, vodIn)) {
                    sendStreamNotFoundStatus(item);
                    throw new StreamNotFoundException(itemName);
                }
                if (!vodIn.subscribe(this, null)) {
                    log.warn("Input source subscribe failed");
                    throw new IOException(String.format("Subscribe to %s failed", itemName));
                }
                firstMessage = setupVodPlayback(item, withReset);
                break;

            default:
                sendStreamNotFoundStatus(item);
                throw new StreamNotFoundException(itemName);
        }

        // Send status notifications to the client
        if (sendNotifications) {
            if (withReset) {
                sendReset();
                sendResetStatus(item);
            }
            sendStartStatus(item);
            if (!withReset) {
                sendSwitchStatus();
            }
            if (item instanceof DynamicPlayItem) {
                sendTransitionStatus();
            }
        }

        // Send the first VOD message (live strategies send their initial data internally)
        if (firstMessage != null) {
            sendMessage(firstMessage);
        }

        subscriberStream.onChange(StreamState.PLAYING, item, !pullMode);

        if (withReset) {
            long now = System.currentTimeMillis();
            playbackStart = now - context.streamOffset;
            nextCheckBufferUnderrun = now + bufferCheckInterval;
            if (itemLength != 0) {
                ensurePullAndPushRunning();
            }
        }
    }

    private int determinePlayDecision(int type, IProviderService.INPUT_TYPE sourceType) {
        switch (type) {
            case -2:
                if (sourceType == IProviderService.INPUT_TYPE.LIVE) {
                    return PLAY_LIVE;
                } else if (sourceType == IProviderService.INPUT_TYPE.VOD) {
                    return PLAY_VOD;
                } else if (sourceType == IProviderService.INPUT_TYPE.LIVE_WAIT) {
                    return PLAY_WAIT;
                }
                break;
            case -1:
                if (sourceType == IProviderService.INPUT_TYPE.LIVE) {
                    return PLAY_LIVE;
                } else if (sourceType == IProviderService.INPUT_TYPE.LIVE_WAIT) {
                    return PLAY_WAIT;
                }
                break;
            case 0:
                // GStreamer rtmp2src compatibility: 0 can be either live or VOD
                if (sourceType == IProviderService.INPUT_TYPE.LIVE) {
                    return PLAY_LIVE;
                } else if (sourceType == IProviderService.INPUT_TYPE.VOD) {
                    return PLAY_VOD;
                }
                break;
            default:
                if (sourceType == IProviderService.INPUT_TYPE.VOD) {
                    return PLAY_VOD;
                }
                break;
        }
        return PLAY_NOT_FOUND;
    }

    private boolean setupLivePlayback(IPlayItem item, IMessageInput in, boolean withReset) throws IOException {
        // Start in keyframe-wait mode so we don't send partial GOPs
        keyframeStrategy.onPlayStart();

        // Check if the publisher has already sent a keyframe (subscriber joining mid-stream)
        boolean keyframeAlreadyAvailable = isKeyframeAvailableInCodec(in);

        if (keyframeAlreadyAvailable) {
            // Send notifications now because LivePlayStrategy.prepare() will immediately
            // send codec data — the notifications must arrive at the client first
            if (withReset) {
                sendReset();
                sendResetStatus(item);
                sendStartStatus(item);
            }
            keyframeStrategy.onKeyframeAlreadyAvailable();
        }

        // Subscribe to receive the live stream's push messages
        in.subscribe(this, null);

        // Create the live strategy and have it send metadata/configs/keyframe
        currentStrategy = new LivePlayStrategy(context, createMessageSender());
        currentStrategy.prepare(item);

        // Return false if we already sent notifications above (avoid double-sending)
        return !keyframeAlreadyAvailable;
    }

    private void setupWaitForLiveStream(String itemName, int type, long itemLength) {
        if (type == -1 && itemLength >= 0) {
            // Wait for the specified duration then end the stream
            waitLiveJob = schedulingService.addScheduledOnceJob(itemLength, new IScheduledJob() {
                public void execute(ISchedulingService service) {
                    connectToProvider(itemName);
                    waitLiveJob = null;
                    subscriberStream.onChange(StreamState.END);
                }
            });
        } else if (type == -2) {
            // Wait up to 15 seconds for the stream to appear
            waitLiveJob = schedulingService.addScheduledOnceJob(15000, new IScheduledJob() {
                public void execute(ISchedulingService service) {
                    connectToProvider(itemName);
                    waitLiveJob = null;
                }
            });
        } else {
            connectToProvider(itemName);
        }
    }

    private RTMPMessage setupVodPlayback(IPlayItem item, boolean withReset) throws IOException {
        if (withReset) {
            releasePendingMessage();
        }
        currentStrategy = new VodPlayStrategy(context, this);
        return currentStrategy.prepare(item);
    }

    private boolean isKeyframeAvailableInCodec(IMessageInput in) {
        if (!(in instanceof IBroadcastScope)) {
            return false;
        }
        IBroadcastScope scope = (IBroadcastScope) in;
        IBroadcastStream stream = (IBroadcastStream) scope.getClientBroadcastStream();
        if (stream == null || stream.getCodecInfo() == null) {
            return false;
        }
        IVideoStreamCodec videoCodec = stream.getCodecInfo().getVideoCodec();
        if (videoCodec == null) {
            return false;
        }
        return videoCodec.getNumInterframes() > 0 || videoCodec.getKeyframe() != null;
    }

    private MessageSender createMessageSender() {
        return new MessageSender() {
            public void sendMessage(RTMPMessage message) {
                PlayEngine.this.sendMessage(message);
            }
        };
    }

    private void connectToProvider(String itemName) {
        log.debug("Attempting connection to {}", itemName);
        IMessageInput in = msgInReference.get();
        if (in == null) {
            in = providerService.getLiveProviderInput(subscriberStream.getScope(), itemName, true);
            msgInReference.set(in);
        }
        if (in != null) {
            log.debug("Provider: {}", msgInReference.get());
            if (in.subscribe(this, null)) {
                log.debug("Subscribed to {} provider", itemName);
                try {
                    currentStrategy = new LivePlayStrategy(context, createMessageSender());
                    currentStrategy.prepare(currentItem.get());
                } catch (IOException e) {
                    log.warn("Could not set up live stream: {}", itemName, e);
                }
            } else {
                log.warn("Subscribe to {} provider failed", itemName);
            }
        } else {
            log.warn("Provider was not found for {}", itemName);
            StreamService.sendNetStreamStatus(subscriberStream.getConnection(), StatusCodes.NS_PLAY_STREAMNOTFOUND, "Stream was not found", itemName, Status.ERROR, streamId);
        }
    }

    /**
     * @param position
     * @throws IllegalStateException
     */
    public void pause(int position) throws IllegalStateException {
        switch (subscriberStream.getState()) {
            case PLAYING:
            case STOPPED:
                subscriberStream.setState(StreamState.STOPPED);
                clearWaitJobs();
                sendPauseStatus(currentItem.get());
                sendClearPing();
                subscriberStream.onChange(StreamState.PAUSED, currentItem.get(), position);
                break;
            default:
                throw new IllegalStateException("Cannot pause in current state");
        }
    }

    /**
     * @param position
     * @throws IllegalStateException
     */
    public void resume(int position) throws IllegalStateException {
        switch (subscriberStream.getState()) {
            case PAUSED:
                subscriberStream.setState(StreamState.PLAYING);
                sendReset();
                sendResumeStatus(currentItem.get());
                if (pullMode) {
                    // VOD: seek to the resume position in the file
                    ((VodPlayStrategy) currentStrategy).seekToPosition(position);
                    subscriberStream.onChange(StreamState.RESUMED, currentItem.get(), position);
                    playbackStart = System.currentTimeMillis() - position;
                    long length = currentItem.get().getLength();
                    if (length >= 0 && (position - context.streamOffset) >= length) {
                        stop();
                    } else {
                        ensurePullAndPushRunning();
                    }
                } else {
                    subscriberStream.onChange(StreamState.RESUMED, currentItem.get(), position);
                    keyframeStrategy.onResume();
                }
                break;
            default:
                throw new IllegalStateException("Cannot resume from non-paused state");
        }
    }

    /**
     * @param position
     * @throws IllegalStateException
     * @throws OperationNotSupportedException
     */
    public void seek(int position) throws IllegalStateException, OperationNotSupportedException {
        pendingOperations.add(new SeekRunnable(position));
        cancelDeferredStop();
        ensurePullAndPushRunning();
    }

    /**
     * @throws IllegalStateException
     */
    public void stop() throws IllegalStateException {
        if (isDebug) {
            log.debug("stop - subscriber stream state: {}", (subscriberStream != null ? subscriberStream.getState() : null));
        }
        switch (subscriberStream.getState()) {
            case PLAYING:
            case PAUSED:
                subscriberStream.setState(StreamState.STOPPED);
                IMessageInput in = msgInReference.get();
                if (in != null && !pullMode) {
                    in.unsubscribe(this);
                    msgInReference.set(null);
                }
                subscriberStream.onChange(StreamState.STOPPED, currentItem.get());
                clearWaitJobs();
                cancelDeferredStop();
                if (subscriberStream instanceof IPlaylistSubscriberStream) {
                    IPlaylistSubscriberStream pss = (IPlaylistSubscriberStream) subscriberStream;
                    if (!pss.hasMoreItems()) {
                        releasePendingMessage();
                        sendCompleteStatus();
                        bytesSent.set(0);
                        sendStopStatus(currentItem.get());
                        sendClearPing();
                    } else {
                        if (lastMessageTs > 0) {
                            // Remember last timestamp so the next playlist item continues from here
                            context.timestampOffset = lastMessageTs;
                        }
                        pss.nextItem();
                    }
                }
                break;
            case CLOSED:
                clearWaitJobs();
                cancelDeferredStop();
                break;
            case STOPPED:
                log.trace("Already in stopped state");
                break;
            default:
                throw new IllegalStateException(String.format("Cannot stop in current state: %s", subscriberStream.getState()));
        }
    }

    /**
     * Close stream
     */
    public void close() {
        if (isDebug) {
            log.debug("close");
        }
        if (!subscriberStream.getState().equals(StreamState.CLOSED)) {
            IMessageInput in = msgInReference.get();
            if (in != null) {
                in.unsubscribe(this);
                msgInReference.set(null);
            }
            subscriberStream.setState(StreamState.CLOSED);
            clearWaitJobs();
            releasePendingMessage();
            lastMessageTs = 0;
            InMemoryPushPushPipe out = (InMemoryPushPushPipe) msgOutReference.get();
            if (out != null) {
                List<IConsumer> consumers = out.getConsumers();
                if (isDebug) {
                    log.debug("Message out consumers: {}", consumers.size());
                }
                if (!consumers.isEmpty()) {
                    for (IConsumer consumer : consumers) {
                        out.unsubscribe(consumer);
                    }
                }
                msgOutReference.set(null);
            }
        } else {
            log.debug("Stream is already in closed state");
        }
    }

    private void sendMessage(RTMPMessage messageIn) {
        IRTMPEvent eventIn = messageIn.getBody();
        int originalTimestamp = eventIn.getTimestamp();
        byte sourceType = eventIn.getSourceType();

        if (isTrace || sourceType != Constants.SOURCE_TYPE_LIVE) {
            log.warn("Source type check - sourceType={} eventType={}", sourceType, eventIn.getClass().getSimpleName());
            long delta = System.currentTimeMillis() - playbackStart;
            log.trace("sendMessage: streamStartTS={}, length={}, streamOffset={}, timestamp={}, lastTs={}, delta={}, buffered={}", context.streamStartTimestamp.get(), currentItem.get().getLength(), context.streamOffset, originalTimestamp, lastMessageTs, delta, lastMessageTs - delta);
        }

        // Ask the strategy if we have played past the requested duration (VOD only)
        if (currentStrategy.hasReachedEnd(currentItem.get(), originalTimestamp)) {
            stop();
            return;
        }

        // Create a copy so we don't modify the source data that others may still need
        IRTMPEvent eventOut = cloneEvent(eventIn);
        eventOut.setSourceType(sourceType);

        // Adjust the timestamp based on the stream type (live vs VOD)
        int adjustedTimestamp = currentStrategy.adjustTimestamp(eventIn, originalTimestamp);

        RTMPMessage messageOut = RTMPMessage.build(eventOut, adjustedTimestamp);

        if (log.isDebugEnabled()) {
            String mediaType = (eventIn instanceof VideoData) ? "VIDEO" : (eventIn instanceof AudioData) ? "AUDIO" : "OTHER";
            log.debug("sendMessage: type={} originalTs={} adjustedTs={} streamStartTS={}", mediaType, originalTimestamp, adjustedTimestamp, context.streamStartTimestamp.get());
        }

        doPushMessage(messageOut);
    }

    private IRTMPEvent cloneEvent(IRTMPEvent eventIn) {
        switch (eventIn.getDataType()) {
            case Constants.TYPE_AGGREGATE:
                return new Aggregate(((Aggregate) eventIn).getData());
            case Constants.TYPE_AUDIO_DATA:
                return new AudioData(((AudioData) eventIn).getData());
            case Constants.TYPE_VIDEO_DATA:
                return new VideoData(((VideoData) eventIn).getData());
            default:
                return new Notify(((Notify) eventIn).getData());
        }
    }

    private void doPushMessage(Status status) {
        StatusMessage message = new StatusMessage();
        message.setBody(status);
        doPushMessage(message);
    }

    private void doPushMessage(AbstractMessage message) {
        if (isTrace) {
            log.trace("doPushMessage: {}", message.getMessageType());
        }
        IMessageOutput out = msgOutReference.get();
        if (out != null) {
            try {
                out.pushMessage(message);
                if (message instanceof RTMPMessage) {
                    IRTMPEvent body = ((RTMPMessage) message).getBody();
                    lastMessageTs = body.getTimestamp();
                    IoBuffer streamData = null;
                    if (body instanceof IStreamData && (streamData = ((IStreamData<?>) body).getData()) != null) {
                        bytesSent.addAndGet(streamData.limit());
                    }
                }
            } catch (IOException err) {
                log.warn("Error while pushing message", err);
            }
        } else {
            log.warn("Push message failed due to null output pipe");
        }
    }

    @Override
    public void pushMessage(IPipe pipe, IMessage message) throws IOException {
        if (!pullMode) {
            if (!context.configsDone) {
                log.debug("Skipping message: live stream setup not complete yet");
                return;
            }
        }

        if (message instanceof RTMPMessage) {
            handleRtmpMessage((RTMPMessage) message);
        } else if (message instanceof ResetMessage) {
            sendReset();
        } else {
            msgOutReference.get().pushMessage(message);
        }
    }

    private void handleRtmpMessage(RTMPMessage rtmpMessage) throws IOException {
        IRTMPEvent body = rtmpMessage.getBody();
        if (!(body instanceof IStreamData)) {
            throw new RuntimeException(String.format("Expected IStreamData but got %s (type %s)", body.getClass(), body.getDataType()));
        }

        String sessionId = subscriberStream.getConnection().getSessionId();
        String streamName = subscriberStream.getBroadcastStreamPublishName();

        // Drop everything if the subscriber is paused
        if (subscriberStream.getState() == StreamState.PAUSED) {
            if (log.isInfoEnabled() && shouldLogPacketDrop()) {
                log.info("Dropping packet because subscriber is paused. sessionId={} stream={} count={}", sessionId, streamName, droppedPacketsCount);
            }
            keyframeStrategy.recordDroppedFrame(rtmpMessage);
            return;
        }

        if (body instanceof VideoData && body.getSourceType() == Constants.SOURCE_TYPE_LIVE) {
            rtmpMessage = handleIncomingVideoFrame(rtmpMessage, sessionId, streamName);
            if (rtmpMessage == null) {
                return; // frame was dropped
            }
        } else if (body instanceof AudioData) {
            rtmpMessage = handleIncomingAudio(rtmpMessage);
            if (rtmpMessage == null) {
                return; // audio disabled
            }
        }

        sendMessage(rtmpMessage);
    }

    private RTMPMessage handleIncomingVideoFrame(RTMPMessage rtmpMessage, String sessionId, String streamName) {
        if (log.isDebugEnabled()) {
            VideoData vd = (VideoData) rtmpMessage.getBody();
            log.debug("Received video: ts={} frameType={} size={}", rtmpMessage.getBody().getTimestamp(), vd.getFrameType(), vd.getData() != null ? vd.getData().limit() : 0);
        }

        IMessageInput msgIn = msgInReference.get();
        if (!(msgIn instanceof IBroadcastScope)) {
            return rtmpMessage; // not a live broadcast, pass through
        }

        IBroadcastStream stream = (IBroadcastStream) ((IBroadcastScope) msgIn).getClientBroadcastStream();
        if (stream == null || stream.getCodecInfo() == null) {
            return rtmpMessage;
        }

        IVideoStreamCodec videoCodec = stream.getCodecInfo().getVideoCodec();
        if (videoCodec == null || !videoCodec.canDropFrames()) {
            return rtmpMessage; // codec doesn't support frame dropping, pass through
        }

        // Drop if client disabled video
        if (!receiveVideo) {
            keyframeStrategy.recordDroppedFrame(rtmpMessage);
            droppedPacketsCount++;
            if (log.isInfoEnabled() && shouldLogPacketDrop()) {
                log.info("Dropped packet: video disabled. sessionId={} stream={} count={}", sessionId, streamName, droppedPacketsCount);
            }
            return null;
        }

        long pendingVideos = pendingVideoMessages();

        // Update sequential pending counter BEFORE making the drop decision
        if (pendingVideos > 1) {
            numSequentialPendingVideoFrames++;
        } else {
            numSequentialPendingVideoFrames = 0;
        }

        if (log.isTraceEnabled()) {
            log.trace("Pending frames: sessionId={} pending={} threshold={} sequential={} dropped={}", sessionId, pendingVideos, maxPendingVideoFrames, numSequentialPendingVideoFrames, droppedPacketsCount);
        }

        // Ask the keyframe strategy whether to send or drop this frame
        boolean shouldSend = keyframeStrategy.shouldSendVideoFrame(rtmpMessage, pendingVideos, maxPendingVideoFrames, maxSequentialPendingVideoFrames, numSequentialPendingVideoFrames);

        if (!shouldSend) {
            droppedPacketsCount++;
            if (log.isInfoEnabled() && shouldLogPacketDrop()) {
                log.info("Dropped packet: keyframe strategy or bandwidth. sessionId={} stream={} dropped={}", sessionId, streamName, droppedPacketsCount);
            }
            // Notify client about bandwidth issues if check interval has passed
            long now = System.currentTimeMillis();
            if (bufferCheckInterval > 0 && now >= nextCheckBufferUnderrun) {
                sendInsufficientBandwidthStatus(currentItem.get());
                nextCheckBufferUnderrun = now + bufferCheckInterval;
            }
            return null;
        }

        if (log.isDebugEnabled()) {
            VideoData vd = (VideoData) rtmpMessage.getBody();
            log.debug("Forwarding video to subscriber: ts={} frameType={} waitingForKeyframe={}", rtmpMessage.getBody().getTimestamp(), vd.getFrameType(), keyframeStrategy.isWaitingForKeyframe());
        }

        // If we have buffered interframes to send (late subscriber catch-up), send those instead
        if (keyframeStrategy.hasPendingBufferedFrames()) {
            VideoData bufferedFrame = keyframeStrategy.getNextBufferedInterframe(videoCodec, rtmpMessage.getBody().getTimestamp());
            if (bufferedFrame != null) {
                return RTMPMessage.build(bufferedFrame);
            }
            // No more buffered frames - fall through to send the live frame
        }

        return rtmpMessage;
    }

    private RTMPMessage handleIncomingAudio(RTMPMessage rtmpMessage) {
        if (log.isDebugEnabled()) {
            log.debug("Received audio: ts={}", rtmpMessage.getBody().getTimestamp());
        }
        if (!receiveAudio && sendBlankAudio) {
            // Send one blank audio packet to reset the player, then disable
            sendBlankAudio = false;
            AudioData blankAudio = new AudioData();
            blankAudio.setTimestamp(lastMessageTs > 0 ? lastMessageTs : 0);
            return RTMPMessage.build(blankAudio);
        } else if (!receiveAudio) {
            return null; // audio is disabled, drop this packet
        }
        return rtmpMessage;
    }

    /** {@inheritDoc} */
    public void onOOBControlMessage(IMessageComponent source, IPipe pipe, OOBControlMessage oobCtrlMsg) {
        if ("ConnectionConsumer".equals(oobCtrlMsg.getTarget())) {
            if (source instanceof IProvider) {
                IMessageOutput out = msgOutReference.get();
                if (out != null) {
                    out.sendOOBControlMessage((IProvider) source, oobCtrlMsg);
                } else {
                    log.warn("Output is not available, message cannot be sent");
                    close();
                }
            }
        }
    }

    /** {@inheritDoc} */
    public void onPipeConnectionEvent(PipeConnectionEvent event) {
        switch (event.getType()) {
            case PROVIDER_CONNECT_PUSH:
                if (event.getProvider() != this) {
                    if (waitLiveJob != null) {
                        schedulingService.removeScheduledJob(waitLiveJob);
                        waitLiveJob = null;
                    }
                    sendPublishedStatus(currentItem.get());
                }
                break;
            case PROVIDER_DISCONNECT:
                if (pullMode) {
                    sendStopStatus(currentItem.get());
                } else {
                    sendUnpublishedStatus(currentItem.get());
                }
                break;
            case CONSUMER_CONNECT_PULL:
                if (event.getConsumer() == this) {
                    pullMode = true;
                }
                break;
            case CONSUMER_CONNECT_PUSH:
                if (event.getConsumer() == this) {
                    pullMode = false;
                }
                break;
            default:
                if (isDebug) {
                    log.debug("Unhandled pipe event: {}", event);
                }
        }
    }

    private boolean okayToSendMessage(IRTMPEvent message) {
        if (message instanceof IStreamData) {
            final long now = System.currentTimeMillis();
            if (isClientBufferFull(now)) {
                return false;
            }
            long pending = pendingMessages();
            if (bufferCheckInterval > 0 && now >= nextCheckBufferUnderrun) {
                if (pending > underrunTrigger) {
                    sendInsufficientBandwidthStatus(currentItem.get());
                }
                nextCheckBufferUnderrun = now + bufferCheckInterval;
            }
            return pending <= underrunTrigger;
        } else {
            String itemName = currentItem.get() != null ? currentItem.get().getName() : "Undefined";
            throw new RuntimeException(String.format("Expected IStreamData but got %s (type %s) for %s", message.getClass(), message.getDataType(), itemName));
        }
    }

    private boolean isClientBufferFull(final long now) {
        if (lastMessageTs > 0) {
            final long delta = now - playbackStart;
            final long buffer = subscriberStream.getClientBufferDuration();
            final long buffered = lastMessageTs - delta;
            log.trace("isClientBufferFull: timestamp={} delta={} buffered={} bufferDuration={}", lastMessageTs, delta, buffered, buffer);
            if (buffer > 0 && buffered > (buffer * 2)) {
                return true;
            }
        }
        return false;
    }

    private boolean isClientBufferEmpty() {
        if (lastMessageTs >= 0) {
            final long delta = System.currentTimeMillis() - playbackStart;
            final long buffered = lastMessageTs - delta;
            log.trace("isClientBufferEmpty: timestamp={} delta={} buffered={}", lastMessageTs, delta, buffered);
            return buffered < 0;
        }
        return false;
    }

    private long pendingVideoMessages() {
        IMessageOutput out = msgOutReference.get();
        if (out != null) {
            OOBControlMessage pendingRequest = new OOBControlMessage();
            pendingRequest.setTarget("ConnectionConsumer");
            pendingRequest.setServiceName("pendingVideoCount");
            out.sendOOBControlMessage(this, pendingRequest);
            if (pendingRequest.getResult() != null) {
                return (Long) pendingRequest.getResult();
            }
        }
        return 0L;
    }

    private long pendingMessages() {
        return subscriberStream.getConnection().getPendingMessages();
    }

    // -------------------------------------------------------------------------
    // Job scheduling helpers
    // -------------------------------------------------------------------------

    private void ensurePullAndPushRunning() {
        log.trace("State should be PLAYING to run this task: {}", subscriberStream.getState());
        if (pullMode && pullAndPush == null && subscriberStream.getState() == StreamState.PLAYING) {
            pullAndPush = subscriberStream.scheduleWithFixedDelay(new PullAndPushRunnable(), 10);
        }
    }

    private void clearWaitJobs() {
        log.debug("Clear wait jobs");
        if (pullAndPush != null) {
            subscriberStream.cancelJob(pullAndPush);
            releasePendingMessage();
            pullAndPush = null;
        }
        if (waitLiveJob != null) {
            schedulingService.removeScheduledJob(waitLiveJob);
            waitLiveJob = null;
        }
    }

    private void runDeferredStop() {
        clearWaitJobs();
        log.trace("Scheduling deferred stop");
        if (deferredStop == null) {
            deferredStop = subscriberStream.scheduleWithFixedDelay(new DeferredStopRunnable(), 100);
        }
    }

    private void cancelDeferredStop() {
        log.debug("Cancel deferred stop");
        if (deferredStop != null) {
            subscriberStream.cancelJob(deferredStop);
            deferredStop = null;
        }
    }

    // -------------------------------------------------------------------------
    // Status message senders
    // -------------------------------------------------------------------------

    private void sendClearPing() {
        Ping eof = new Ping();
        eof.setEventType(PingType.STREAM_PLAYBUFFER_CLEAR);
        eof.setValue2(streamId);
        doPushMessage(RTMPMessage.build(eof));
    }

    private void sendReset() {
        if (pullMode) {
            Ping recorded = new Ping();
            recorded.setEventType(PingType.RECORDED_STREAM);
            recorded.setValue2(streamId);
            doPushMessage(RTMPMessage.build(recorded));
        }
        Ping begin = new Ping();
        begin.setEventType(PingType.STREAM_BEGIN);
        begin.setValue2(streamId);
        doPushMessage(RTMPMessage.build(begin));
        doPushMessage(new ResetMessage());
    }

    private void sendResetStatus(IPlayItem item) {
        Status reset = new Status(StatusCodes.NS_PLAY_RESET);
        reset.setClientid(streamId);
        reset.setDetails(item.getName());
        reset.setDesciption(String.format("Playing and resetting %s.", item.getName()));
        doPushMessage(reset);
    }

    private void sendStartStatus(IPlayItem item) {
        Status start = new Status(StatusCodes.NS_PLAY_START);
        start.setClientid(streamId);
        start.setDetails(item.getName());
        start.setDesciption(String.format("Started playing %s.", item.getName()));
        doPushMessage(start);
    }

    private void sendStopStatus(IPlayItem item) {
        Status stop = new Status(StatusCodes.NS_PLAY_STOP);
        stop.setClientid(streamId);
        stop.setDesciption(String.format("Stopped playing %s.", item.getName()));
        stop.setDetails(item.getName());
        doPushMessage(stop);
    }

    private void sendOnPlayStatus(String code, int duration, long bytes) {
        if (isDebug) {
            log.debug("Sending onPlayStatus - code={} duration={} bytes={}", code, duration, bytes);
        }
        IoBuffer buf = IoBuffer.allocate(102);
        buf.setAutoExpand(true);
        Output out = new Output(buf);
        out.writeString("onPlayStatus");
        ObjectMap<Object, Object> args = new ObjectMap<>();
        args.put("code", code);
        args.put("level", Status.STATUS);
        args.put("duration", duration);
        args.put("bytes", bytes);
        String name = currentItem.get().getName();
        if (StatusCodes.NS_PLAY_TRANSITION_COMPLETE.equals(code)) {
            args.put("clientId", streamId);
            args.put("details", name);
            args.put("description", String.format("Transitioned to %s", name));
            args.put("isFastPlay", false);
        }
        out.writeObject(args);
        buf.flip();
        Notify event = new Notify(buf, "onPlayStatus");
        event.setTimestamp(lastMessageTs > 0 ? lastMessageTs : 0);
        doPushMessage(RTMPMessage.build(event));
    }

    private void sendSwitchStatus() {
        sendOnPlayStatus(StatusCodes.NS_PLAY_SWITCH, 1, bytesSent.get());
    }

    private void sendTransitionStatus() {
        sendOnPlayStatus(StatusCodes.NS_PLAY_TRANSITION_COMPLETE, 0, bytesSent.get());
    }

    private void sendCompleteStatus() {
        int duration = (lastMessageTs > 0) ? Math.max(0, lastMessageTs - context.streamStartTimestamp.get()) : 0;
        if (isDebug) {
            log.debug("sendCompleteStatus - duration={} bytes={}", duration, bytesSent.get());
        }
        sendOnPlayStatus(StatusCodes.NS_PLAY_COMPLETE, duration, bytesSent.get());
    }

    private void sendSeekStatus(IPlayItem item, int position) {
        Status seek = new Status(StatusCodes.NS_SEEK_NOTIFY);
        seek.setClientid(streamId);
        seek.setDetails(item.getName());
        seek.setDesciption(String.format("Seeking %d (stream ID: %d).", position, streamId));
        doPushMessage(seek);
    }

    private void sendPauseStatus(IPlayItem item) {
        Status pause = new Status(StatusCodes.NS_PAUSE_NOTIFY);
        pause.setClientid(streamId);
        pause.setDetails(item.getName());
        doPushMessage(pause);
    }

    private void sendResumeStatus(IPlayItem item) {
        Status resume = new Status(StatusCodes.NS_UNPAUSE_NOTIFY);
        resume.setClientid(streamId);
        resume.setDetails(item.getName());
        doPushMessage(resume);
    }

    private void sendPublishedStatus(IPlayItem item) {
        Status published = new Status(StatusCodes.NS_PLAY_PUBLISHNOTIFY);
        published.setClientid(streamId);
        published.setDetails(item.getName());
        doPushMessage(published);
    }

    private void sendUnpublishedStatus(IPlayItem item) {
        Status unpublished = new Status(StatusCodes.NS_PLAY_UNPUBLISHNOTIFY);
        unpublished.setClientid(streamId);
        unpublished.setDetails(item.getName());
        doPushMessage(unpublished);
    }

    private void sendStreamNotFoundStatus(IPlayItem item) {
        Status notFound = new Status(StatusCodes.NS_PLAY_STREAMNOTFOUND);
        notFound.setClientid(streamId);
        notFound.setLevel(Status.ERROR);
        notFound.setDetails(item.getName());
        doPushMessage(notFound);
    }

    private void sendInsufficientBandwidthStatus(IPlayItem item) {
        Status insufficientBW = new Status(StatusCodes.NS_PLAY_INSUFFICIENT_BW);
        insufficientBW.setClientid(streamId);
        insufficientBW.setLevel(Status.WARNING);
        insufficientBW.setDetails(item.getName());
        insufficientBW.setDesciption("Data is playing behind the normal speed");
        doPushMessage(insufficientBW);
    }

    // -------------------------------------------------------------------------
    // Utility helpers
    // -------------------------------------------------------------------------

    private void releasePendingMessage() {
        if (pendingMessage != null) {
            IRTMPEvent body = pendingMessage.getBody();
            if (body instanceof IStreamData && ((IStreamData<?>) body).getData() != null) {
                ((IStreamData<?>) body).getData().free();
            }
            pendingMessage = null;
        }
    }

    protected boolean checkSendMessageEnabled(RTMPMessage message) {
        IRTMPEvent body = message.getBody();
        if (!receiveAudio && body instanceof AudioData) {
            ((IStreamData<?>) body).getData().free();
            if (sendBlankAudio) {
                sendBlankAudio = false;
                body = new AudioData();
                body.setTimestamp(lastMessageTs >= 0 ? lastMessageTs - context.timestampOffset : -context.timestampOffset);
                message = RTMPMessage.build(body);
            } else {
                return false;
            }
        } else if (!receiveVideo && body instanceof VideoData) {
            ((IStreamData<?>) body).getData().free();
            return false;
        }
        return true;
    }

    private boolean shouldLogPacketDrop() {
        long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        if (now - droppedPacketsCountLastLogTimestamp > droppedPacketsCountLogInterval) {
            droppedPacketsCountLastLogTimestamp = now;
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Public getters
    // -------------------------------------------------------------------------

    public boolean isPullMode() {
        return pullMode;
    }

    public boolean isPaused() {
        return subscriberStream.isPaused();
    }

    public int getLastMessageTimestamp() {
        return lastMessageTs;
    }

    public long getPlaybackStart() {
        return playbackStart;
    }

    public void sendBlankAudio(boolean sendBlankAudio) {
        this.sendBlankAudio = sendBlankAudio;
    }

    public boolean receiveAudio() {
        return receiveAudio;
    }

    public boolean receiveAudio(boolean receive) {
        boolean oldValue = receiveAudio;
        if (receiveAudio != receive) {
            receiveAudio = receive;
        }
        return oldValue;
    }

    public boolean receiveVideo() {
        return receiveVideo;
    }

    public boolean receiveVideo(boolean receive) {
        boolean oldValue = receiveVideo;
        if (receiveVideo != receive) {
            receiveVideo = receive;
        }
        return oldValue;
    }

    // -------------------------------------------------------------------------
    // Inner classes: scheduled jobs
    // -------------------------------------------------------------------------

    private final class SeekRunnable implements Runnable {

        private final int position;

        SeekRunnable(int position) {
            this.position = position;
        }

        @SuppressWarnings("incomplete-switch")
        public void run() {
            log.trace("Seek: {}", position);
            boolean startPullPushThread = false;
            switch (subscriberStream.getState()) {
                case PLAYING:
                    startPullPushThread = true;
                case PAUSED:
                case STOPPED:
                    if (!pullMode) {
                        throw new RuntimeException("Seek not supported in push mode");
                    }
                    releasePendingMessage();
                    clearWaitJobs();
                    break;
                default:
                    throw new IllegalStateException("Cannot seek in current state");
            }

            sendClearPing();
            sendReset();
            sendSeekStatus(currentItem.get(), position);
            sendStartStatus(currentItem.get());

            // Ask the VOD strategy to seek to the nearest keyframe
            VodPlayStrategy vodStrategy = (VodPlayStrategy) currentStrategy;
            int seekPos = vodStrategy.seekToPosition(position);
            if (seekPos == -1) {
                seekPos = position;
            }

            log.trace("Playback start before seek: {}", playbackStart);
            playbackStart = System.currentTimeMillis() - seekPos;
            log.trace("Playback start after seek: {} seek pos: {}", playbackStart, seekPos);
            subscriberStream.onChange(StreamState.SEEK, currentItem.get(), seekPos);

            boolean messageSent = false;

            // Send a video keyframe snapshot if we are paused or stopped
            switch (subscriberStream.getState()) {
                case PAUSED:
                case STOPPED:
                    if (vodStrategy.hasVideo()) {
                        IMessage msg = null;
                        IMessageInput in = msgInReference.get();
                        do {
                            try {
                                msg = in.pullMessage();
                            } catch (Throwable err) {
                                log.warn("Error while pulling message", err);
                                break;
                            }
                            if (msg instanceof RTMPMessage) {
                                RTMPMessage rtmpMessage = (RTMPMessage) msg;
                                IRTMPEvent body = rtmpMessage.getBody();
                                if (body instanceof VideoData && ((VideoData) body).getFrameType() == org.red5.codec.VideoFrameType.KEYFRAME) {
                                    doPushMessage(rtmpMessage);
                                    rtmpMessage.getBody().release();
                                    messageSent = true;
                                    lastMessageTs = body.getTimestamp();
                                    break;
                                }
                            }
                        } while (msg != null);
                    }
                    break;
                default:
                    break;
            }

            // Check if we seeked past the end of the stream
            long length = currentItem.get().getLength();
            if (length >= 0 && (position - context.streamOffset) >= length) {
                stop();
            }

            // If no keyframe was found, send a blank audio packet to confirm the seek
            if (!messageSent) {
                log.debug("Sending blank audio packet to confirm seek");
                AudioData audio = new AudioData();
                audio.setTimestamp(seekPos);
                audio.setHeader(new Header());
                audio.getHeader().setTimer(seekPos);
                RTMPMessage audioMessage = RTMPMessage.build(audio);
                lastMessageTs = seekPos;
                doPushMessage(audioMessage);
                audioMessage.getBody().release();
            }
            if (!messageSent && subscriberStream.getState() == StreamState.PLAYING) {
                prefillClientBufferAfterSeek(seekPos, vodStrategy);
            }

            if (startPullPushThread) {
                ensurePullAndPushRunning();
            }
        }

        private void prefillClientBufferAfterSeek(int seekPos, VodPlayStrategy vodStrategy) {
            if (!vodStrategy.hasVideo()) {
                return;
            }
            boolean isRtmpt = subscriberStream.getConnection().getProtocol().equals("rtmpt");
            final long clientBuffer = subscriberStream.getClientBufferDuration();
            IMessage msg = null;
            IMessageInput in = msgInReference.get();
            int msgSent = 0;
            do {
                try {
                    msg = in.pullMessage();
                    if (msg instanceof RTMPMessage) {
                        RTMPMessage rtmpMessage = (RTMPMessage) msg;
                        IRTMPEvent body = rtmpMessage.getBody();
                        if (body.getTimestamp() >= seekPos + (clientBuffer * 2)) {
                            releasePendingMessage();
                            if (checkSendMessageEnabled(rtmpMessage)) {
                                pendingMessage = rtmpMessage;
                            }
                            break;
                        }
                        if (!checkSendMessageEnabled(rtmpMessage)) {
                            continue;
                        }
                        msgSent++;
                        sendMessage(rtmpMessage);
                    }
                } catch (Throwable err) {
                    log.warn("Error while pulling message during seek buffer fill", err);
                    break;
                }
            } while (!isRtmpt && (msg != null));
            log.trace("prefillClientBuffer: sent {} messages", msgSent);
            playbackStart = System.currentTimeMillis() - lastMessageTs;
        }

    }

    private final class PullAndPushRunnable implements IScheduledJob {

        public void execute(ISchedulingService svc) {
            if (!pushPullRunning.compareAndSet(false, true)) {
                log.debug("Push/pull already running, skipping this tick");
                return;
            }
            try {
                // Execute any pending operations (e.g., seeks) first
                executePendingOperations();

                // Pull messages from VOD and send them to the client
                if (subscriberStream.getState() == StreamState.PLAYING && pullMode) {
                    pushNextVodMessages();
                }
            } catch (IOException err) {
                log.warn("Error while getting message", err);
                runDeferredStop();
            } finally {
                pushPullRunning.compareAndSet(true, false);
            }
        }

        private void executePendingOperations() {
            Runnable worker;
            while (!pendingOperations.isEmpty()) {
                log.debug("Pending operations: {}", pendingOperations.size());
                worker = pendingOperations.remove();
                // If multiple seek operations are queued, skip to the last one
                while (worker instanceof SeekRunnable && pendingOperations.peek() instanceof SeekRunnable) {
                    worker = pendingOperations.remove();
                }
                if (worker != null) {
                    log.debug("Executing pending operation: {}", worker);
                    worker.run();
                }
            }
        }

        private void pushNextVodMessages() throws IOException {
            if (pendingMessage != null) {
                IRTMPEvent body = pendingMessage.getBody();
                if (okayToSendMessage(body)) {
                    sendMessage(pendingMessage);
                    releasePendingMessage();
                }
                return;
            }

            IMessage msg;
            IMessageInput in = msgInReference.get();
            do {
                msg = in.pullMessage();
                if (msg instanceof RTMPMessage) {
                    RTMPMessage rtmpMessage = (RTMPMessage) msg;
                    if (checkSendMessageEnabled(rtmpMessage)) {
                        IRTMPEvent body = rtmpMessage.getBody();
                        body.setTimestamp(body.getTimestamp() + context.timestampOffset);
                        if (okayToSendMessage(body)) {
                            log.trace("Sending VOD message: ts={}", rtmpMessage.getBody().getTimestamp());
                            sendMessage(rtmpMessage);
                            IoBuffer data = ((IStreamData<?>) body).getData();
                            if (data != null) {
                                data.free();
                            }
                        } else {
                            pendingMessage = rtmpMessage;
                            ensurePullAndPushRunning();
                            break;
                        }
                    }
                } else if (msg == null) {
                    log.debug("No more messages to send (end of VOD)");
                    runDeferredStop();
                }
            } while (msg != null);
        }
    }

    private class DeferredStopRunnable implements IScheduledJob {

        public void execute(ISchedulingService service) {
            if (isClientBufferEmpty()) {
                log.trace("Client buffer is empty, stopping stream");
                stop();
            }
        }

    }

}
