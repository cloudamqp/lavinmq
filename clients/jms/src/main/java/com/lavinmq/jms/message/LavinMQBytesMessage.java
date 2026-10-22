package com.lavinmq.jms.message;

import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotReadableException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public final class LavinMQBytesMessage extends LavinMQMessage implements BytesMessage {
    private ByteArrayOutputStream writeBuffer;
    private DataOutputStream out;
    private byte[] data = new byte[0];
    private DataInputStream in;

    public LavinMQBytesMessage() {
        startWriting();
    }

    /** A received message: read-only. */
    public LavinMQBytesMessage(byte[] body) {
        this.data = body;
        this.bodyReadOnly = true;
        this.in = new DataInputStream(new ByteArrayInputStream(data));
    }

    private void startWriting() {
        writeBuffer = new ByteArrayOutputStream();
        out = new DataOutputStream(writeBuffer);
        in = null;
    }

    /** The complete body, whatever the current mode. */
    public byte[] bytes() {
        if (out != null) {
            try {
                out.flush();
            } catch (IOException ignored) {
                // in-memory stream
            }
            return writeBuffer.toByteArray();
        }
        return data;
    }

    private DataInputStream reader() throws MessageNotReadableException {
        if (in == null) throw new MessageNotReadableException("message is in write-only mode; call reset() first");
        return in;
    }

    private DataOutputStream writer() throws JMSException {
        checkBodyWriteable();
        if (out == null) throw new jakarta.jms.MessageNotWriteableException("message is in read-only mode");
        return out;
    }

    private static JMSException io(IOException e) {
        if (e instanceof EOFException) return new MessageEOFException("end of message body");
        JMSException j = new JMSException(e.getMessage());
        j.initCause(e);
        return j;
    }

    @Override
    public long getBodyLength() throws JMSException {
        reader();
        return data.length;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        try {
            return reader().readBoolean();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public byte readByte() throws JMSException {
        try {
            return reader().readByte();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        try {
            return reader().readUnsignedByte();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public short readShort() throws JMSException {
        try {
            return reader().readShort();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        try {
            return reader().readUnsignedShort();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public char readChar() throws JMSException {
        try {
            return reader().readChar();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public int readInt() throws JMSException {
        try {
            return reader().readInt();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public long readLong() throws JMSException {
        try {
            return reader().readLong();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public float readFloat() throws JMSException {
        try {
            return reader().readFloat();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public double readDouble() throws JMSException {
        try {
            return reader().readDouble();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public String readUTF() throws JMSException {
        try {
            return reader().readUTF();
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        if (length < 0 || length > value.length) throw new IndexOutOfBoundsException("length " + length);
        try {
            DataInputStream r = reader();
            int total = 0;
            while (total < length) {
                int n = r.read(value, total, length - total);
                if (n < 0) break;
                total += n;
            }
            return total == 0 && length > 0 ? -1 : total;
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        try {
            writer().writeBoolean(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        try {
            writer().writeByte(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        try {
            writer().writeShort(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        try {
            writer().writeChar(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        try {
            writer().writeInt(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        try {
            writer().writeLong(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        try {
            writer().writeFloat(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        try {
            writer().writeDouble(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        try {
            writer().writeUTF(value);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        writeBytes(value, 0, value.length);
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        try {
            writer().write(value, offset, length);
        } catch (IOException e) {
            throw io(e);
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) throw new NullPointerException("null cannot be written to a BytesMessage");
        if (value instanceof Boolean) writeBoolean((Boolean) value);
        else if (value instanceof Byte) writeByte((Byte) value);
        else if (value instanceof Short) writeShort((Short) value);
        else if (value instanceof Character) writeChar((Character) value);
        else if (value instanceof Integer) writeInt((Integer) value);
        else if (value instanceof Long) writeLong((Long) value);
        else if (value instanceof Float) writeFloat((Float) value);
        else if (value instanceof Double) writeDouble((Double) value);
        else if (value instanceof String) writeUTF((String) value);
        else if (value instanceof byte[]) writeBytes((byte[]) value);
        else throw new MessageFormatException("unsupported type " + value.getClass().getName());
    }

    @Override
    public void reset() throws JMSException {
        data = bytes();
        out = null;
        writeBuffer = null;
        bodyReadOnly = true;
        in = new DataInputStream(new ByteArrayInputStream(data));
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        data = new byte[0];
        startWriting();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        byte[] b = bytes();
        if (b.length == 0) return null;
        if (c.isAssignableFrom(byte[].class)) {
            if (in == null) reset();
            else in = new DataInputStream(new ByteArrayInputStream(data));
            return (T) b.clone();
        }
        throw bodyMismatch(c, "byte[]");
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        return bytes().length == 0 || c.isAssignableFrom(byte[].class);
    }

    @Override
    public String messageType() {
        return "bytes";
    }
}
