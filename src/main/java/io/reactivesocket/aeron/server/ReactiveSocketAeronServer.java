package io.reactivesocket.aeron.server;

import io.reactivesocket.Completable;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.observable.Observer;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.reactivesocket.aeron.internal.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.internal.Constants.SERVER_STREAM_ID;

public class ReactiveSocketAeronServer implements AutoCloseable, Loggable {
    private final Aeron aeron;

    private final int port;

    private final ConcurrentHashMap<Integer, AeronServerDuplexConnection> connections;

    private final Scheduler.Worker worker;

    private final Subscription subscription;

    private volatile boolean running = true;

    private final ConnectionSetupHandler connectionSetupHandler;

    private final LeaseGovernor leaseGovernor;

    private final ConcurrentHashMap<Integer, ReactiveSocket> sockets;

    private final CountDownLatch shutdownLatch;

    private ReactiveSocketAeronServer(String host, int port, ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        this.port = port;
        this.connections = new ConcurrentHashMap<>();
        this.connectionSetupHandler = connectionSetupHandler;
        this.leaseGovernor = leaseGovernor;
        this.sockets = new ConcurrentHashMap<>();
        this.shutdownLatch = new CountDownLatch(1);

        final Aeron.Context ctx = new Aeron.Context();
        ctx.newImageHandler(this::newImageHandler);
        ctx.errorHandler(t ->  error(t.getMessage(), t));

        aeron = Aeron.connect(ctx);

        final String serverChannel =  "udp://" + host + ":" + port;
        info("Start new ReactiveSocketAeronServer on channel {}", serverChannel);
        subscription = aeron.addSubscription(serverChannel, SERVER_STREAM_ID);

        final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

        worker = Schedulers.computation().createWorker();
        poll(fragmentAssembler);

    }

    public static ReactiveSocketAeronServer create(String host, int port, ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        return new ReactiveSocketAeronServer(host, port, connectionSetupHandler, leaseGovernor);
    }

    public static ReactiveSocketAeronServer create(int port, ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        return create("127.0.0.1", port, connectionSetupHandler, leaseGovernor);
    }

    public static ReactiveSocketAeronServer create(ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        return create(39790, connectionSetupHandler, leaseGovernor);
    }

    public static ReactiveSocketAeronServer create(String host, int port, ConnectionSetupHandler connectionSetupHandler) {
        return new ReactiveSocketAeronServer(host, port, connectionSetupHandler, LeaseGovernor.NULL_LEASE_GOVERNOR);
    }

    public static ReactiveSocketAeronServer create(int port, ConnectionSetupHandler connectionSetupHandler) {
        return create("127.0.0.1", port, connectionSetupHandler, LeaseGovernor.NULL_LEASE_GOVERNOR);
    }

    public static ReactiveSocketAeronServer create(ConnectionSetupHandler connectionSetupHandler) {
        return create(39790, connectionSetupHandler, LeaseGovernor.NULL_LEASE_GOVERNOR);
    }

    void poll(FragmentAssembler fragmentAssembler) {
       if (running) {
            worker.schedule(() -> {
                try {
                    subscription.poll(fragmentAssembler, Integer.MAX_VALUE);
                    poll(fragmentAssembler);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            });
        } else {
           shutdownLatch.countDown();
       }
    }

    void fragmentHandler(DirectBuffer buffer, int offset, int length, Header header) {
        final int sessionId = header.sessionId();

        int messageTypeInt = buffer.getInt(offset);
        MessageType type = MessageType.from(messageTypeInt);

        if (MessageType.FRAME == type) {
            AeronServerDuplexConnection connection = connections.get(sessionId);
            if (connection != null) {
                Observer<Frame> subscriber = connection.getSubscriber();
                ByteBuffer bytes = ByteBuffer.allocate(length);
                buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
                final Frame frame = Frame.from(bytes);
                subscriber.onNext(frame);
            }
        } else if (MessageType.ESTABLISH_CONNECTION_REQUEST == type) {
            final long start = System.nanoTime();
            AeronServerDuplexConnection connection = null;
            debug("Looking a connection to ack establish connection for session id => {}", sessionId);
            while (connection == null) {
                final long current = System.nanoTime();

                if (current - start > TimeUnit.SECONDS.toNanos(30)) {
                    throw new RuntimeException("unable to find connection to ack establish connection for session id => " + sessionId);
                }

                connection = connections.get(sessionId);
            }
            debug("Found a connection to ack establish connection for session id => {}", sessionId);
            connection.ackEstablishConnection(sessionId);
        }

    }

    void newImageHandler(Image image, String channel, int streamId, int sessionId, long joiningPosition, String sourceIdentity) {
        if (SERVER_STREAM_ID == streamId) {
            debug("Handling new image for session id => {} and stream id => {}", streamId, sessionId);
            final AeronServerDuplexConnection connection = connections.computeIfAbsent(sessionId, (_s) -> {
                final String responseChannel = "udp://" + sourceIdentity.substring(0, sourceIdentity.indexOf(':')) + ":" + port;
                Publication publication = aeron.addPublication(responseChannel, CLIENT_STREAM_ID);
                debug("Creating new connection for responseChannel => {}, streamId => {}, and sessionId => {}", responseChannel, streamId, sessionId);
                return new AeronServerDuplexConnection(publication);
            });
            debug("Accepting ReactiveSocket connection");
            ReactiveSocket socket = ReactiveSocket.fromServerConnection(
                connection,
                connectionSetupHandler,
                leaseGovernor,
                error -> error(error.getMessage(), error));

            sockets.put(sessionId, socket);

            socket.start(new Completable() {
                @Override
                public void success() {

                }

                @Override
                public void error(Throwable e) {

                }
            });
        } else {
            debug("Unsupported stream id {}", streamId);
        }
    }

    @Override
    public void close() throws Exception {
        running = false;

        shutdownLatch.await(30, TimeUnit.SECONDS);

        worker.unsubscribe();
        aeron.close();

        for (AeronServerDuplexConnection connection : connections.values()) {
            connection.close();
        }

        for (ReactiveSocket reactiveSocket : sockets.values()) {
            reactiveSocket.close();
        }
    }

}