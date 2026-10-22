package com.lavinmq.jms.message;

import jakarta.jms.MessageFormatException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Self-describing binary encoding of MapMessage and StreamMessage bodies.
 *
 * <p>Map: magic {@code LMQM}, u8 version, u32 count, then (UTF name, value)*. Stream: magic {@code LMQS}, u8 version,
 * values until the end. A value is one tag byte (0 null, 1 boolean, 2 byte, 3 short, 4 char, 5 int, 6 long, 7 float,
 * 8 double, 9 String as u32 length + UTF-8, 10 byte[] as u32 length + bytes) followed by its big-endian payload.
 */
public final class TypedValueCodec {
    static final byte[] MAP_MAGIC = {'L', 'M', 'Q', 'M'};
    static final byte[] STREAM_MAGIC = {'L', 'M', 'Q', 'S'};
    static final int VERSION = 1;

    private TypedValueCodec() {
    }

    public static byte[] encodeMap(Map<String, Object> map) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.write(MAP_MAGIC);
            out.writeByte(VERSION);
            out.writeInt(map.size());
            for (Map.Entry<String, Object> e : map.entrySet()) {
                byte[] name = e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(name.length);
                out.write(name);
                writeValue(out, e.getValue());
            }
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Map<String, Object> decodeMap(byte[] body) throws MessageFormatException {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            readMagic(in, MAP_MAGIC, "map");
            int n = in.readInt();
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                int len = in.readInt();
                byte[] name = new byte[len];
                in.readFully(name);
                map.put(new String(name, java.nio.charset.StandardCharsets.UTF_8), readValue(in));
            }
            return map;
        } catch (IOException e) {
            throw new MessageFormatException("malformed map message body: " + e.getMessage());
        }
    }

    public static byte[] encodeStream(List<Object> values) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.write(STREAM_MAGIC);
            out.writeByte(VERSION);
            for (Object v : values) writeValue(out, v);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static List<Object> decodeStream(byte[] body) throws MessageFormatException {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            readMagic(in, STREAM_MAGIC, "stream");
            List<Object> values = new ArrayList<>();
            while (in.available() > 0) values.add(readValue(in));
            return values;
        } catch (IOException e) {
            throw new MessageFormatException("malformed stream message body: " + e.getMessage());
        }
    }

    private static void readMagic(DataInputStream in, byte[] magic, String what) throws IOException, MessageFormatException {
        byte[] m = new byte[4];
        in.readFully(m);
        if (!java.util.Arrays.equals(m, magic)) throw new MessageFormatException("not a " + what + " message body");
        int version = in.readUnsignedByte();
        if (version != VERSION) throw new MessageFormatException("unsupported " + what + " message body version " + version);
    }

    static void writeValue(DataOutputStream out, Object v) throws IOException {
        if (v == null) {
            out.writeByte(0);
        } else if (v instanceof Boolean) {
            out.writeByte(1);
            out.writeBoolean((Boolean) v);
        } else if (v instanceof Byte) {
            out.writeByte(2);
            out.writeByte((Byte) v);
        } else if (v instanceof Short) {
            out.writeByte(3);
            out.writeShort((Short) v);
        } else if (v instanceof Character) {
            out.writeByte(4);
            out.writeChar((Character) v);
        } else if (v instanceof Integer) {
            out.writeByte(5);
            out.writeInt((Integer) v);
        } else if (v instanceof Long) {
            out.writeByte(6);
            out.writeLong((Long) v);
        } else if (v instanceof Float) {
            out.writeByte(7);
            out.writeFloat((Float) v);
        } else if (v instanceof Double) {
            out.writeByte(8);
            out.writeDouble((Double) v);
        } else if (v instanceof String) {
            byte[] b = ((String) v).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeByte(9);
            out.writeInt(b.length);
            out.write(b);
        } else if (v instanceof byte[]) {
            byte[] b = (byte[]) v;
            out.writeByte(10);
            out.writeInt(b.length);
            out.write(b);
        } else {
            throw new IllegalArgumentException("unsupported value type " + v.getClass().getName());
        }
    }

    static Object readValue(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        switch (tag) {
            case 0: return null;
            case 1: return in.readBoolean();
            case 2: return in.readByte();
            case 3: return in.readShort();
            case 4: return in.readChar();
            case 5: return in.readInt();
            case 6: return in.readLong();
            case 7: return in.readFloat();
            case 8: return in.readDouble();
            case 9: {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                return new String(b, java.nio.charset.StandardCharsets.UTF_8);
            }
            case 10: {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                return b;
            }
            default: throw new IOException("unknown value tag " + tag);
        }
    }
}
