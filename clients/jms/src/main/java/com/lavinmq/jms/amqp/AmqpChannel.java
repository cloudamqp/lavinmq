package com.lavinmq.jms.amqp;

import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.codec.ContentHeader;
import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.FrameType;
import com.lavinmq.jms.amqp.method.BasicMethods;
import com.lavinmq.jms.amqp.method.ChannelMethods;
import com.lavinmq.jms.amqp.method.ConfirmMethods;
import com.lavinmq.jms.amqp.method.ExchangeMethods;
import com.lavinmq.jms.amqp.method.Method;
import com.lavinmq.jms.amqp.method.MethodDecoder;
import com.lavinmq.jms.amqp.method.QueueMethods;
import com.lavinmq.jms.amqp.method.TxMethods;
import com.lavinmq.jms.amqp.transport.ContentAssembler;
import com.lavinmq.jms.amqp.transport.FrameWriter;
import com.lavinmq.jms.amqp.transport.SerialExecutor;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

/** One AMQP channel. Synchronous methods block until the server replies; events arrive on the dispatch thread. */
public final class AmqpChannel {
    private static final Logger LOG = System.getLogger(AmqpChannel.class.getName());
    private static final Object GET_EMPTY = new Object();

    private final AmqpConnection connection;
    private final int id;
    private final FrameWriter writer;
    private final SerialExecutor dispatcher;
    private final ReentrantLock rpcLock = new ReentrantLock();
    private final ContentAssembler assembler = new ContentAssembler();
    private final ConcurrentHashMap<String, ConsumerCallback> consumers = new ConcurrentHashMap<>();
    private final AtomicReference<ShutdownSignal> shutdownCause = new AtomicReference<>();
    private volatile ChannelListener listener;
    private volatile Rpc rpc;
    private volatile ConsumerCallback pendingConsumer;
    private volatile boolean confirmMode;
    private long publishSeq;

    private static final class Rpc {
        final Set<Class<?>> accepted;
        final CompletableFuture<Object> future = new CompletableFuture<>();

        Rpc(Set<Class<?>> accepted) {
            this.accepted = accepted;
        }

        boolean accepts(Object reply) {
            for (Class<?> c : accepted) if (c.isInstance(reply)) return true;
            return false;
        }
    }

    AmqpChannel(AmqpConnection connection, int id) {
        this.connection = connection;
        this.id = id;
        this.writer = connection.writer();
        this.dispatcher = new SerialExecutor(connection.dispatchPool());
    }

    public int id() {
        return id;
    }

    public AmqpConnection connection() {
        return connection;
    }

    public boolean isOpen() {
        return shutdownCause.get() == null;
    }

    public ShutdownSignal shutdownCause() {
        return shutdownCause.get();
    }

    public void setListener(ChannelListener l) {
        this.listener = l;
    }

    public boolean confirmMode() {
        return confirmMode;
    }

    // ---- synchronous methods -------------------------------------------------------------------------------------

    void open() throws IOException {
        rpc(new ChannelMethods.Open(), ChannelMethods.OpenOk.class);
    }

    public void exchangeDeclare(String name, String type, boolean passive, boolean durable, boolean autoDelete, boolean internal,
                                Map<String, Object> args) throws IOException {
        rpc(new ExchangeMethods.Declare(name, type, passive, durable, autoDelete, internal, false, args), ExchangeMethods.DeclareOk.class);
    }

    public void exchangeDelete(String name, boolean ifUnused) throws IOException {
        rpc(new ExchangeMethods.Delete(name, ifUnused, false), ExchangeMethods.DeleteOk.class);
    }

    public void exchangeBind(String destination, String source, String routingKey, Map<String, Object> args) throws IOException {
        rpc(new ExchangeMethods.Bind(destination, source, routingKey, false, args), ExchangeMethods.BindOk.class);
    }

    public void exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> args) throws IOException {
        rpc(new ExchangeMethods.Unbind(destination, source, routingKey, false, args), ExchangeMethods.UnbindOk.class);
    }

    public QueueMethods.DeclareOk queueDeclare(String name, boolean passive, boolean durable, boolean exclusive, boolean autoDelete,
                                               Map<String, Object> args) throws IOException {
        return rpc(new QueueMethods.Declare(name, passive, durable, exclusive, autoDelete, false, args), QueueMethods.DeclareOk.class);
    }

    public long queueDelete(String name, boolean ifUnused, boolean ifEmpty) throws IOException {
        return rpc(new QueueMethods.Delete(name, ifUnused, ifEmpty, false), QueueMethods.DeleteOk.class).messageCount();
    }

    public void queueBind(String queue, String exchange, String routingKey, Map<String, Object> args) throws IOException {
        rpc(new QueueMethods.Bind(queue, exchange, routingKey, false, args), QueueMethods.BindOk.class);
    }

    public void queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> args) throws IOException {
        rpc(new QueueMethods.Unbind(queue, exchange, routingKey, args), QueueMethods.UnbindOk.class);
    }

    public long queuePurge(String name) throws IOException {
        return rpc(new QueueMethods.Purge(name, false), QueueMethods.PurgeOk.class).messageCount();
    }

    public void basicQos(int prefetchCount, boolean global) throws IOException {
        rpc(new BasicMethods.Qos(0, prefetchCount, global), BasicMethods.QosOk.class);
    }

    /** Starts a consumer and returns the (possibly server-generated) consumer tag. */
    public String basicConsume(String queue, String consumerTag, boolean noAck, boolean exclusive, Map<String, Object> args,
                               ConsumerCallback callback) throws IOException {
        rpcLock.lock();
        try {
            String tag = consumerTag == null ? "" : consumerTag;
            // The reader thread registers the callback under the server's tag when consume-ok arrives, before it
            // reads the next frame, so the first delivery can never find the consumer missing.
            pendingConsumer = callback;
            try {
                return rpc(new BasicMethods.Consume(queue, tag, false, noAck, exclusive, false, args), BasicMethods.ConsumeOk.class).consumerTag();
            } finally {
                pendingConsumer = null;
            }
        } finally {
            rpcLock.unlock();
        }
    }

    public void basicCancel(String consumerTag) throws IOException {
        try {
            rpc(new BasicMethods.Cancel(consumerTag, false), BasicMethods.CancelOk.class);
        } finally {
            consumers.remove(consumerTag);
        }
    }

    /** Publishes a message. Returns the publish sequence number in confirm mode, otherwise 0. */
    public long basicPublish(String exchange, String routingKey, boolean mandatory, BasicProperties props, byte[] body) throws IOException {
        return basicPublish(exchange, routingKey, mandatory, props, body, null);
    }

    /**
     * Publishes a message. In confirm mode {@code onSeq} (if given) receives the sequence number before the frames
     * are written, while the write lock is held, so a confirm can never arrive before the number is registered.
     */
    public long basicPublish(String exchange, String routingKey, boolean mandatory, BasicProperties props, byte[] body,
                             LongConsumer onSeq) throws IOException {
        checkOpen();
        if (props == null) props = BasicProperties.EMPTY;
        if (body == null) body = new byte[0];
        writer.lock();
        try {
            checkOpen();
            long seq = 0;
            if (confirmMode) {
                seq = ++publishSeq;
                if (onSeq != null) onSeq.accept(seq);
            }
            writer.writeContent(id, new BasicMethods.Publish(exchange, routingKey, mandatory, false), props, body);
            return seq;
        } finally {
            writer.unlock();
        }
    }

    /** Fetches one message, or returns null when the queue is empty. */
    public GetResponse basicGet(String queue, boolean noAck) throws IOException {
        Object r = rpc(new BasicMethods.Get(queue, noAck), Set.of(GetResponse.class, BasicMethods.GetEmpty.class));
        return r instanceof GetResponse ? (GetResponse) r : null;
    }

    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        send(new BasicMethods.Ack(deliveryTag, multiple));
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        send(new BasicMethods.Nack(deliveryTag, multiple, requeue));
    }

    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        send(new BasicMethods.Reject(deliveryTag, requeue));
    }

    public void confirmSelect() throws IOException {
        rpcLock.lock();
        try {
            rpc(new ConfirmMethods.Select(false), ConfirmMethods.SelectOk.class);
            writer.lock();
            try {
                confirmMode = true;
                publishSeq = 0;
            } finally {
                writer.unlock();
            }
        } finally {
            rpcLock.unlock();
        }
    }

    public long nextPublishSeqNo() {
        writer.lock();
        try {
            return publishSeq + 1;
        } finally {
            writer.unlock();
        }
    }

    public void txSelect() throws IOException {
        rpc(new TxMethods.Select(), TxMethods.SelectOk.class);
    }

    public void txCommit() throws IOException {
        rpc(new TxMethods.Commit(), TxMethods.CommitOk.class);
    }

    public void txRollback() throws IOException {
        rpc(new TxMethods.Rollback(), TxMethods.RollbackOk.class);
    }

    public void close() throws IOException {
        close(200, "Goodbye");
    }

    public void close(int replyCode, String replyText) throws IOException {
        if (shutdownCause.get() != null) return;
        ShutdownSignal signal = ShutdownSignal.application(false, replyCode, replyText);
        try {
            rpc(new ChannelMethods.Close(replyCode, replyText, 0, 0), ChannelMethods.CloseOk.class);
        } catch (ChannelClosedException | ConnectionClosedException e) {
            // closed concurrently by the peer: that is still a closed channel
        } finally {
            if (shutdownCause.get() == null) {
                connection.channelClosed(id);
                processShutdown(signal);
            }
        }
    }

    // ---- plumbing ------------------------------------------------------------------------------------------------

    private void checkOpen() throws IOException {
        ShutdownSignal s = shutdownCause.get();
        if (s != null) throw s.isHardError() ? new ConnectionClosedException(s) : new ChannelClosedException(s);
    }

    private void send(Method m) throws IOException {
        checkOpen();
        writer.write(Frame.method(id, m));
    }

    private <T extends Method> T rpc(Method m, Class<T> expected) throws IOException {
        return expected.cast(rpc(m, Set.of(expected)));
    }

    private Object rpc(Method m, Set<Class<?>> accepted) throws IOException {
        rpcLock.lock();
        try {
            checkOpen();
            Rpc k = new Rpc(accepted);
            rpc = k;
            try {
                writer.write(Frame.method(id, m));
                return k.future.get(connection.config().rpcTimeoutMs(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                Throwable c = e.getCause();
                if (c instanceof IOException) throw (IOException) c;
                throw new AmqpException("rpc " + m.name() + " failed", c);
            } catch (TimeoutException e) {
                RpcTimeoutException te = new RpcTimeoutException("no reply to " + m.name() + " within " + connection.config().rpcTimeoutMs() + " ms");
                abortLocally(ShutdownSignal.fromCause(te, false));
                throw te;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AmqpException("interrupted while waiting for reply to " + m.name());
            } finally {
                rpc = null;
            }
        } finally {
            rpcLock.unlock();
        }
    }

    /** Local teardown when the channel state is unknown (RPC timeout). The id is not reused. */
    private void abortLocally(ShutdownSignal signal) {
        connection.channels().remove(id);
        try {
            writer.write(Frame.method(id, new ChannelMethods.Close(541, "INTERNAL_ERROR - rpc timeout", 0, 0)));
        } catch (IOException ignored) {
            // best effort
        }
        processShutdown(signal);
    }

    /** Called on the reader thread for every frame addressed to this channel. Never blocks on user code. */
    void handleFrame(Frame f) throws IOException {
        switch (f.type()) {
            case FrameType.METHOD:
                handleMethod(MethodDecoder.decode(f.payload()));
                break;
            case FrameType.HEADER:
                if (assembler.onHeader(ContentHeader.read(new AmqpReader(f.payload())))) contentComplete();
                break;
            case FrameType.BODY:
                if (assembler.onBody(f.payload())) contentComplete();
                break;
            default:
                throw new ProtocolException("unexpected frame type " + f.type() + " on channel " + id);
        }
    }

    private void handleMethod(Method m) throws IOException {
        if (m.hasContent()) {
            assembler.onMethod(m);
            return;
        }
        if (m instanceof BasicMethods.GetEmpty) {
            completeRpc(m);
        } else if (m instanceof BasicMethods.Ack) {
            BasicMethods.Ack a = (BasicMethods.Ack) m;
            ChannelListener l = listener;
            if (l != null) dispatcher.execute(() -> l.onConfirm(true, a.deliveryTag(), a.multiple()));
        } else if (m instanceof BasicMethods.Nack) {
            BasicMethods.Nack n = (BasicMethods.Nack) m;
            ChannelListener l = listener;
            if (l != null) dispatcher.execute(() -> l.onConfirm(false, n.deliveryTag(), n.multiple()));
        } else if (m instanceof BasicMethods.Cancel) {
            BasicMethods.Cancel c = (BasicMethods.Cancel) m;
            ConsumerCallback cb = consumers.remove(c.consumerTag());
            if (!c.noWait()) {
                writer.write(Frame.method(id, new BasicMethods.CancelOk(c.consumerTag())));
            }
            if (cb != null) dispatcher.execute(() -> cb.handleCancel(c.consumerTag()));
        } else if (m instanceof ChannelMethods.Flow) {
            boolean active = ((ChannelMethods.Flow) m).active();
            writer.write(Frame.method(id, new ChannelMethods.FlowOk(active)));
            ChannelListener l = listener;
            if (l != null) dispatcher.execute(() -> l.onFlow(active));
        } else if (m instanceof ChannelMethods.Close) {
            ChannelMethods.Close c = (ChannelMethods.Close) m;
            connection.channelClosed(id);
            try {
                writer.write(Frame.method(id, new ChannelMethods.CloseOk()));
            } catch (IOException ignored) {
                // connection is going down
            }
            processShutdown(ShutdownSignal.fromClose(c));
        } else {
            if (m instanceof BasicMethods.ConsumeOk) {
                ConsumerCallback cb = pendingConsumer;
                if (cb != null) consumers.put(((BasicMethods.ConsumeOk) m).consumerTag(), cb);
            }
            completeRpc(m);
        }
    }

    private void completeRpc(Object reply) throws ProtocolException {
        Rpc k = rpc;
        if (k != null && k.accepts(reply)) {
            k.future.complete(reply);
            return;
        }
        String name = reply instanceof Method ? ((Method) reply).name() : reply.getClass().getSimpleName();
        throw new ProtocolException("unexpected " + name + " on channel " + id + (k == null ? " (no rpc pending)" : ""));
    }

    private void contentComplete() throws ProtocolException {
        Object msg = assembler.build();
        if (msg instanceof Delivery) {
            Delivery d = (Delivery) msg;
            ConsumerCallback cb = consumers.get(d.consumerTag());
            if (cb == null) {
                LOG.log(Level.DEBUG, "delivery for unknown consumer {0} on channel {1} dropped", d.consumerTag(), id);
                return;
            }
            dispatcher.execute(() -> cb.handleDelivery(d));
        } else if (msg instanceof GetResponse) {
            completeRpc(msg);
        } else if (msg instanceof ReturnedMessage) {
            ChannelListener l = listener;
            if (l != null) dispatcher.execute(() -> l.onReturn((ReturnedMessage) msg));
        }
    }

    /** Marks the channel closed, fails the pending RPC and notifies consumers and the listener (after queued events). */
    void processShutdown(ShutdownSignal signal) {
        if (!shutdownCause.compareAndSet(null, signal)) return;
        Rpc k = rpc;
        if (k != null) {
            k.future.completeExceptionally(signal.isHardError() ? new ConnectionClosedException(signal) : new ChannelClosedException(signal));
        }
        Map<String, ConsumerCallback> snapshot = Map.copyOf(consumers);
        consumers.clear();
        ChannelListener l = listener;
        dispatcher.execute(() -> {
            for (Map.Entry<String, ConsumerCallback> e : snapshot.entrySet()) {
                try {
                    e.getValue().handleShutdown(e.getKey(), signal);
                } catch (RuntimeException ex) {
                    LOG.log(Level.WARNING, "consumer shutdown callback threw", ex);
                }
            }
            if (l != null) l.onShutdown(signal);
        });
    }

    @Override
    public String toString() {
        return "AmqpChannel[" + id + " on " + connection + "]";
    }
}
