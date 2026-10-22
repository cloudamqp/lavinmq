package com.lavinmq.jms.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

/** Deserializes ObjectMessage bodies with the context class loader and a deserialization filter. */
public final class ObjectMessageDeserializer {
    public static final String DEFAULT_FILTER = "java.base/*;java.lang.*;java.util.*;java.math.*;java.time.*;!*";

    private ObjectMessageDeserializer() {
    }

    public static Serializable deserialize(byte[] bytes, String filterPattern) throws IOException, ClassNotFoundException {
        ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(filterPattern == null ? DEFAULT_FILTER : filterPattern);
        ObjectInputFilter global = ObjectInputFilter.Config.getSerialFilter();
        ObjectInputFilter effective = global == null ? filter : ObjectInputFilter.merge(filter, global);
        try (ObjectInputStream in = new ContextClassLoaderObjectInputStream(new ByteArrayInputStream(bytes))) {
            in.setObjectInputFilter(effective);
            return (Serializable) in.readObject();
        }
    }

    private static final class ContextClassLoaderObjectInputStream extends ObjectInputStream {
        ContextClassLoaderObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            ClassLoader tccl = Thread.currentThread().getContextClassLoader();
            if (tccl != null) {
                try {
                    return Class.forName(desc.getName(), false, tccl);
                } catch (ClassNotFoundException ignored) {
                    // fall back to the default resolution
                }
            }
            return super.resolveClass(desc);
        }
    }
}
