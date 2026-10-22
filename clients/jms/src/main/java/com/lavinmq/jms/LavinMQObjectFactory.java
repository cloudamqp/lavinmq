package com.lavinmq.jms;

import java.util.Enumeration;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/** JNDI object factory that rebuilds a {@link LavinMQConnectionFactory} from its {@link Reference}. */
public class LavinMQObjectFactory implements ObjectFactory {
    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) {
        if (!(obj instanceof Reference)) return null;
        Reference ref = (Reference) obj;
        if (!LavinMQConnectionFactory.class.getName().equals(ref.getClassName())) return null;
        LavinMQConnectionFactory f = new LavinMQConnectionFactory();
        Enumeration<RefAddr> addrs = ref.getAll();
        while (addrs.hasMoreElements()) {
            RefAddr a = addrs.nextElement();
            if (a.getContent() != null) f.applyProperty(a.getType(), a.getContent().toString());
        }
        return f;
    }
}
