package com.lavinmq.jms.message;

import jakarta.jms.JMSException;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.StreamMessage;
import java.util.ArrayList;
import java.util.List;

public final class LavinMQStreamMessage extends LavinMQMessage implements StreamMessage {
    private final List<Object> values = new ArrayList<>();
    private int pos;
    /** Remaining bytes of a byte[] field that is being read in chunks with readBytes. */
    private byte[] partialBytes;
    private int partialOffset;

    public LavinMQStreamMessage() {
    }

    public LavinMQStreamMessage(List<Object> values) {
        this.values.addAll(values);
        this.bodyReadOnly = true;
    }

    public List<Object> valuesView() {
        return new ArrayList<>(values);
    }

    private Object next() throws JMSException {
        if (!bodyReadOnly) throw new MessageNotReadableException("message is in write-only mode; call reset() first");
        if (partialBytes != null) throw new MessageFormatException("a byte[] field is being read; finish it with readBytes first");
        if (pos >= values.size()) throw new MessageEOFException("end of stream");
        return values.get(pos);
    }

    private <T> T take(T converted) {
        pos++;
        return converted;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        return take(Conversions.toBoolean(next()));
    }

    @Override
    public byte readByte() throws JMSException {
        return take(Conversions.toByte(next()));
    }

    @Override
    public short readShort() throws JMSException {
        return take(Conversions.toShort(next()));
    }

    @Override
    public char readChar() throws JMSException {
        return take(Conversions.toChar(next()));
    }

    @Override
    public int readInt() throws JMSException {
        return take(Conversions.toInt(next()));
    }

    @Override
    public long readLong() throws JMSException {
        return take(Conversions.toLong(next()));
    }

    @Override
    public float readFloat() throws JMSException {
        return take(Conversions.toFloat(next()));
    }

    @Override
    public double readDouble() throws JMSException {
        return take(Conversions.toDouble(next()));
    }

    @Override
    public String readString() throws JMSException {
        return take(Conversions.toString(next()));
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        if (partialBytes == null) {
            if (!bodyReadOnly) throw new MessageNotReadableException("message is in write-only mode; call reset() first");
            if (pos >= values.size()) throw new MessageEOFException("end of stream");
            Object v = values.get(pos);
            if (v == null) {
                pos++;
                return -1;
            }
            if (!(v instanceof byte[])) throw new MessageFormatException("field is not a byte[]");
            partialBytes = (byte[]) v;
            partialOffset = 0;
            pos++;
            if (partialBytes.length == 0) {
                partialBytes = null;
                return 0;
            }
        }
        int remaining = partialBytes.length - partialOffset;
        if (remaining == 0) {
            partialBytes = null;
            return -1;
        }
        int n = Math.min(remaining, value.length);
        System.arraycopy(partialBytes, partialOffset, value, 0, n);
        partialOffset += n;
        if (n < value.length) partialBytes = null;
        return n;
    }

    @Override
    public Object readObject() throws JMSException {
        Object v = next();
        pos++;
        return v instanceof byte[] ? ((byte[]) v).clone() : v;
    }

    private void write(Object v) throws JMSException {
        checkBodyWriteable();
        values.add(v);
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        write(value);
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        write(value);
    }

    @Override
    public void writeShort(short value) throws JMSException {
        write(value);
    }

    @Override
    public void writeChar(char value) throws JMSException {
        write(value);
    }

    @Override
    public void writeInt(int value) throws JMSException {
        write(value);
    }

    @Override
    public void writeLong(long value) throws JMSException {
        write(value);
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        write(value);
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        write(value);
    }

    @Override
    public void writeString(String value) throws JMSException {
        write(value);
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        write(value.clone());
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        byte[] copy = new byte[length];
        System.arraycopy(value, offset, copy, 0, length);
        write(copy);
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        write(Conversions.checkObject(value, true));
    }

    @Override
    public void reset() {
        bodyReadOnly = true;
        pos = 0;
        partialBytes = null;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        values.clear();
        pos = 0;
        partialBytes = null;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        throw new MessageFormatException("StreamMessage bodies cannot be retrieved with getBody");
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        return false;
    }

    @Override
    public String messageType() {
        return "stream";
    }
}
