package com.lavinmq.jms;

import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.message.MessageCodec;
import jakarta.jms.ConnectionMetaData;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public final class LavinMQConnectionMetaData implements ConnectionMetaData {
    private final String providerVersion;

    LavinMQConnectionMetaData(String providerVersion) {
        this.providerVersion = providerVersion == null ? "unknown" : providerVersion;
    }

    @Override
    public String getJMSVersion() {
        return "3.1";
    }

    @Override
    public int getJMSMajorVersion() {
        return 3;
    }

    @Override
    public int getJMSMinorVersion() {
        return 1;
    }

    @Override
    public String getJMSProviderName() {
        return "LavinMQ";
    }

    @Override
    public String getProviderVersion() {
        return providerVersion;
    }

    @Override
    public int getProviderMajorVersion() {
        return versionPart(0);
    }

    @Override
    public int getProviderMinorVersion() {
        return versionPart(1);
    }

    private int versionPart(int i) {
        String[] parts = providerVersion.split("[.-]");
        try {
            return parts.length > i ? Integer.parseInt(parts[i]) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public Enumeration<String> getJMSXPropertyNames() {
        return Collections.enumeration(List.of(MessageCodec.JMSX_DELIVERY_COUNT, MessageCodec.JMSX_GROUP_ID,
            MessageCodec.JMSX_GROUP_SEQ, MessageCodec.JMSX_USER_ID, MessageCodec.JMSX_APP_ID));
    }

    /** The version of this client library. */
    public String getClientVersion() {
        return ConnectionConfig.VERSION;
    }
}
