package com.lavinmq.jms.amqp.codec;

/** Test helper for golden byte comparisons. */
public final class Hex {
    private Hex() {
    }

    public static byte[] bytes(String hex) {
        String clean = hex.replace(" ", "");
        byte[] out = new byte[clean.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(clean.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(String.format("%02X", x & 0xFF));
        }
        return sb.toString();
    }
}
