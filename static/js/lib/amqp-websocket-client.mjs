class AMQPError extends Error {
    constructor(message, connection) {
        super(message);
        this.name = "AMQPError";
        this.connection = connection;
    }
}

class AMQPView extends DataView {
    getUint64(byteOffset, littleEndian) {
        const left = this.getUint32(byteOffset, littleEndian);
        const right = this.getUint32(byteOffset + 4, littleEndian);
        const combined = littleEndian ? left + 2 ** 32 * right : 2 ** 32 * left + right;
        if (!Number.isSafeInteger(combined))
            console.warn(combined, 'exceeds MAX_SAFE_INTEGER. Precision may be lost');
        return combined;
    }
    setUint64(byteOffset, value, littleEndian) {
        this.setBigUint64(byteOffset, BigInt(value), littleEndian);
    }
    getInt64(byteOffset, littleEndian) {
        return Number(this.getBigInt64(byteOffset, littleEndian));
    }
    setInt64(byteOffset, value, littleEndian) {
        this.setBigInt64(byteOffset, BigInt(value), littleEndian);
    }
    getShortString(byteOffset) {
        const len = this.getUint8(byteOffset);
        byteOffset += 1;
        if (typeof Buffer !== "undefined") {
            const text = Buffer.from(this.buffer, this.byteOffset + byteOffset, len).toString();
            return [text, len + 1];
        }
        else {
            const view = new Uint8Array(this.buffer, this.byteOffset + byteOffset, len);
            const text = AMQPView.decoder.decode(view);
            return [text, len + 1];
        }
    }
    setShortString(byteOffset, string) {
        if (typeof Buffer !== "undefined") {
            const len = Buffer.byteLength(string);
            if (len > 255)
                throw new Error(`Short string too long, ${len} bytes: ${string.substring(0, 255)}...`);
            this.setUint8(byteOffset, len);
            byteOffset += 1;
            Buffer.from(this.buffer, this.byteOffset + byteOffset, len).write(string);
            return len + 1;
        }
        else {
            const utf8 = AMQPView.encoder.encode(string);
            const len = utf8.byteLength;
            if (len > 255)
                throw new Error(`Short string too long, ${len} bytes: ${string.substring(0, 255)}...`);
            this.setUint8(byteOffset, len);
            byteOffset += 1;
            const view = new Uint8Array(this.buffer, this.byteOffset + byteOffset);
            view.set(utf8);
            return len + 1;
        }
    }
    getLongString(byteOffset, littleEndian) {
        const len = this.getUint32(byteOffset, littleEndian);
        byteOffset += 4;
        if (typeof Buffer !== "undefined") {
            const text = Buffer.from(this.buffer, this.byteOffset + byteOffset, len).toString();
            return [text, len + 4];
        }
        else {
            const view = new Uint8Array(this.buffer, this.byteOffset + byteOffset, len);
            const text = AMQPView.decoder.decode(view);
            return [text, len + 4];
        }
    }
    setLongString(byteOffset, string, littleEndian) {
        if (typeof Buffer !== "undefined") {
            const len = Buffer.byteLength(string);
            this.setUint32(byteOffset, len, littleEndian);
            byteOffset += 4;
            Buffer.from(this.buffer, this.byteOffset + byteOffset, len).write(string);
            return len + 4;
        }
        else {
            const utf8 = AMQPView.encoder.encode(string);
            const len = utf8.byteLength;
            this.setUint32(byteOffset, len, littleEndian);
            byteOffset += 4;
            const view = new Uint8Array(this.buffer, this.byteOffset + byteOffset);
            view.set(utf8);
            return len + 4;
        }
    }
    getProperties(byteOffset, littleEndian) {
        let j = byteOffset;
        const flags = this.getUint16(j, littleEndian);
        j += 2;
        const props = {};
        if ((flags & 0x8000) > 0) {
            const [contentType, len] = this.getShortString(j);
            j += len;
            props.contentType = contentType;
        }
        if ((flags & 0x4000) > 0) {
            const [contentEncoding, len] = this.getShortString(j);
            j += len;
            props.contentEncoding = contentEncoding;
        }
        if ((flags & 0x2000) > 0) {
            const [headers, len] = this.getTable(j, littleEndian);
            j += len;
            props.headers = headers;
        }
        if ((flags & 0x1000) > 0) {
            props.deliveryMode = this.getUint8(j);
            j += 1;
        }
        if ((flags & 0x0800) > 0) {
            props.priority = this.getUint8(j);
            j += 1;
        }
        if ((flags & 0x0400) > 0) {
            const [correlationId, len] = this.getShortString(j);
            j += len;
            props.correlationId = correlationId;
        }
        if ((flags & 0x0200) > 0) {
            const [replyTo, len] = this.getShortString(j);
            j += len;
            props.replyTo = replyTo;
        }
        if ((flags & 0x0100) > 0) {
            const [expiration, len] = this.getShortString(j);
            j += len;
            props.expiration = expiration;
        }
        if ((flags & 0x0080) > 0) {
            const [messageId, len] = this.getShortString(j);
            j += len;
            props.messageId = messageId;
        }
        if ((flags & 0x0040) > 0) {
            props.timestamp = new Date(this.getInt64(j, littleEndian) * 1000);
            j += 8;
        }
        if ((flags & 0x0020) > 0) {
            const [type, len] = this.getShortString(j);
            j += len;
            props.type = type;
        }
        if ((flags & 0x0010) > 0) {
            const [userId, len] = this.getShortString(j);
            j += len;
            props.userId = userId;
        }
        if ((flags & 0x0008) > 0) {
            const [appId, len] = this.getShortString(j);
            j += len;
            props.appId = appId;
        }
        const len = j - byteOffset;
        return [props, len];
    }
    setProperties(byteOffset, properties, littleEndian) {
        let j = byteOffset;
        let flags = 0;
        if (properties.contentType)
            flags = flags | 0x8000;
        if (properties.contentEncoding)
            flags = flags | 0x4000;
        if (properties.headers)
            flags = flags | 0x2000;
        if (properties.deliveryMode)
            flags = flags | 0x1000;
        if (properties.priority)
            flags = flags | 0x0800;
        if (properties.correlationId)
            flags = flags | 0x0400;
        if (properties.replyTo)
            flags = flags | 0x0200;
        if (properties.expiration)
            flags = flags | 0x0100;
        if (properties.messageId)
            flags = flags | 0x0080;
        if (properties.timestamp)
            flags = flags | 0x0040;
        if (properties.type)
            flags = flags | 0x0020;
        if (properties.userId)
            flags = flags | 0x0010;
        if (properties.appId)
            flags = flags | 0x0008;
        this.setUint16(j, flags, littleEndian);
        j += 2;
        if (properties.contentType) {
            j += this.setShortString(j, properties.contentType);
        }
        if (properties.contentEncoding) {
            j += this.setShortString(j, properties.contentEncoding);
        }
        if (properties.headers) {
            j += this.setTable(j, properties.headers);
        }
        if (properties.deliveryMode) {
            this.setUint8(j, properties.deliveryMode);
            j += 1;
        }
        if (properties.priority) {
            this.setUint8(j, properties.priority);
            j += 1;
        }
        if (properties.correlationId) {
            j += this.setShortString(j, properties.correlationId);
        }
        if (properties.replyTo) {
            j += this.setShortString(j, properties.replyTo);
        }
        if (properties.expiration) {
            j += this.setShortString(j, properties.expiration);
        }
        if (properties.messageId) {
            j += this.setShortString(j, properties.messageId);
        }
        if (properties.timestamp) {
            const unixEpoch = Math.floor(Number(properties.timestamp) / 1000);
            this.setInt64(j, unixEpoch, littleEndian);
            j += 8;
        }
        if (properties.type) {
            j += this.setShortString(j, properties.type);
        }
        if (properties.userId) {
            j += this.setShortString(j, properties.userId);
        }
        if (properties.appId) {
            j += this.setShortString(j, properties.appId);
        }
        const len = j - byteOffset;
        return len;
    }
    getTable(byteOffset, littleEndian) {
        const table = {};
        let i = byteOffset;
        const len = this.getUint32(byteOffset, littleEndian);
        i += 4;
        for (; i < byteOffset + 4 + len;) {
            const [k, strLen] = this.getShortString(i);
            i += strLen;
            const [v, vLen] = this.getField(i, littleEndian);
            i += vLen;
            table[k] = v;
        }
        return [table, len + 4];
    }
    setTable(byteOffset, table, littleEndian) {
        let i = byteOffset + 4;
        for (const [key, value] of Object.entries(table)) {
            if (value === undefined)
                continue;
            i += this.setShortString(i, key);
            i += this.setField(i, value, littleEndian);
        }
        this.setUint32(byteOffset, i - byteOffset - 4, littleEndian);
        return i - byteOffset;
    }
    getField(byteOffset, littleEndian) {
        let i = byteOffset;
        const k = this.getUint8(i);
        i += 1;
        const type = String.fromCharCode(k);
        let v;
        let len;
        switch (type) {
            case 't':
                v = this.getUint8(i) === 1;
                i += 1;
                break;
            case 'b':
                v = this.getInt8(i);
                i += 1;
                break;
            case 'B':
                v = this.getUint8(i);
                i += 1;
                break;
            case 's':
                v = this.getInt16(i, littleEndian);
                i += 2;
                break;
            case 'u':
                v = this.getUint16(i, littleEndian);
                i += 2;
                break;
            case 'I':
                v = this.getInt32(i, littleEndian);
                i += 4;
                break;
            case 'i':
                v = this.getUint32(i, littleEndian);
                i += 4;
                break;
            case 'l':
                v = this.getInt64(i, littleEndian);
                i += 8;
                break;
            case 'f':
                v = this.getFloat32(i, littleEndian);
                i += 4;
                break;
            case 'd':
                v = this.getFloat64(i, littleEndian);
                i += 8;
                break;
            case 'S':
                [v, len] = this.getLongString(i, littleEndian);
                i += len;
                break;
            case 'F':
                [v, len] = this.getTable(i, littleEndian);
                i += len;
                break;
            case 'A':
                [v, len] = this.getArray(i, littleEndian);
                i += len;
                break;
            case 'x':
                [v, len] = this.getByteArray(i, littleEndian);
                i += len;
                break;
            case 'T':
                v = new Date(this.getInt64(i, littleEndian) * 1000);
                i += 8;
                break;
            case 'V':
                v = null;
                break;
            case 'D': {
                const scale = this.getUint8(i);
                i += 1;
                const value = this.getUint32(i, littleEndian);
                i += 4;
                v = value / 10 ** scale;
                break;
            }
            default:
                throw `Field type '${k}' not supported`;
        }
        return [v, i - byteOffset];
    }
    setField(byteOffset, field, littleEndian) {
        let i = byteOffset;
        switch (typeof field) {
            case "string":
                this.setUint8(i, 'S'.charCodeAt(0));
                i += 1;
                i += this.setLongString(i, field, littleEndian);
                break;
            case "boolean":
                this.setUint8(i, 't'.charCodeAt(0));
                i += 1;
                this.setUint8(i, field ? 1 : 0);
                i += 1;
                break;
            case "bigint":
                this.setUint8(i, 'l'.charCodeAt(0));
                i += 1;
                this.setBigInt64(i, field, littleEndian);
                i += 8;
                break;
            case "number":
                if (Number.isInteger(field)) {
                    if (-(2 ** 32) < field && field < 2 ** 32) {
                        this.setUint8(i, 'I'.charCodeAt(0));
                        i += 1;
                        this.setInt32(i, field, littleEndian);
                        i += 4;
                    }
                    else {
                        this.setUint8(i, 'l'.charCodeAt(0));
                        i += 1;
                        this.setInt64(i, field, littleEndian);
                        i += 8;
                    }
                }
                else {
                    if (-(2 ** 32) < field && field < 2 ** 32) {
                        this.setUint8(i, 'f'.charCodeAt(0));
                        i += 1;
                        this.setFloat32(i, field, littleEndian);
                        i += 4;
                    }
                    else {
                        this.setUint8(i, 'd'.charCodeAt(0));
                        i += 1;
                        this.setFloat64(i, field, littleEndian);
                        i += 8;
                    }
                }
                break;
            case "object":
                if (Array.isArray(field)) {
                    this.setUint8(i, 'A'.charCodeAt(0));
                    i += 1;
                    i += this.setArray(i, field, littleEndian);
                }
                else if (field instanceof Uint8Array) {
                    this.setUint8(i, 'x'.charCodeAt(0));
                    i += 1;
                    i += this.setByteArray(i, field);
                }
                else if (field instanceof ArrayBuffer) {
                    this.setUint8(i, 'x'.charCodeAt(0));
                    i += 1;
                    i += this.setByteArray(i, new Uint8Array(field));
                }
                else if (field instanceof Date) {
                    this.setUint8(i, 'T'.charCodeAt(0));
                    i += 1;
                    const unixEpoch = Math.floor(Number(field) / 1000);
                    this.setInt64(i, unixEpoch, littleEndian);
                    i += 8;
                }
                else if (field === null || field === undefined) {
                    this.setUint8(i, 'V'.charCodeAt(0));
                    i += 1;
                }
                else {
                    this.setUint8(i, 'F'.charCodeAt(0));
                    i += 1;
                    i += this.setTable(i, field, littleEndian);
                }
                break;
            default:
                throw `Unsupported field type '${field}'`;
        }
        return i - byteOffset;
    }
    getArray(byteOffset, littleEndian) {
        const len = this.getUint32(byteOffset, littleEndian);
        byteOffset += 4;
        const endOffset = byteOffset + len;
        const v = [];
        for (; byteOffset < endOffset;) {
            const [field, fieldLen] = this.getField(byteOffset, littleEndian);
            byteOffset += fieldLen;
            v.push(field);
        }
        return [v, len + 4];
    }
    setArray(byteOffset, array, littleEndian) {
        const start = byteOffset;
        byteOffset += 4;
        array.forEach((e) => {
            byteOffset += this.setField(byteOffset, e, littleEndian);
        });
        this.setUint32(start, byteOffset - start - 4, littleEndian);
        return byteOffset - start;
    }
    getByteArray(byteOffset, littleEndian) {
        const len = this.getUint32(byteOffset, littleEndian);
        byteOffset += 4;
        const v = new Uint8Array(this.buffer, this.byteOffset + byteOffset, len);
        return [v, len + 4];
    }
    setByteArray(byteOffset, data, littleEndian) {
        this.setUint32(byteOffset, data.byteLength, littleEndian);
        byteOffset += 4;
        const view = new Uint8Array(this.buffer, this.byteOffset + byteOffset, data.byteLength);
        view.set(data);
        return data.byteLength + 4;
    }
}
AMQPView.decoder = new TextDecoder();
AMQPView.encoder = new TextEncoder();

class AMQPQueue {
    constructor(channel, name) {
        this.channel = channel;
        this.name = name;
    }
    bind(exchange, routingKey = "", args = {}) {
        return new Promise((resolve, reject) => {
            this.channel.queueBind(this.name, exchange, routingKey, args)
                .then(() => resolve(this))
                .catch(reject);
        });
    }
    unbind(exchange, routingKey = "", args = {}) {
        return new Promise((resolve, reject) => {
            this.channel.queueUnbind(this.name, exchange, routingKey, args)
                .then(() => resolve(this))
                .catch(reject);
        });
    }
    publish(body, properties = {}) {
        return new Promise((resolve, reject) => {
            this.channel.basicPublish("", this.name, body, properties)
                .then(() => resolve(this))
                .catch(reject);
        });
    }
    subscribe({ noAck = true, exclusive = false, tag = "", args = {} } = {}, callback) {
        return this.channel.basicConsume(this.name, { noAck, exclusive, tag, args }, callback);
    }
    unsubscribe(consumerTag) {
        return new Promise((resolve, reject) => {
            this.channel.basicCancel(consumerTag)
                .then(() => resolve(this))
                .catch(reject);
        });
    }
    delete() {
        return new Promise((resolve, reject) => {
            this.channel.queueDelete(this.name)
                .then(() => resolve(this))
                .catch(reject);
        });
    }
    get({ noAck = true } = {}) {
        return this.channel.basicGet(this.name, { noAck });
    }
    purge() {
        return this.channel.queuePurge(this.name);
    }
}

class AMQPConsumer {
    constructor(channel, tag, onMessage) {
        this.closed = false;
        this.channel = channel;
        this.tag = tag;
        this.onMessage = onMessage;
    }
    wait(timeout) {
        if (this.closedError)
            return Promise.reject(this.closedError);
        if (this.closed)
            return Promise.resolve();
        return new Promise((resolve, reject) => {
            this.resolveWait = resolve;
            this.rejectWait = reject;
            if (timeout) {
                const onTimeout = () => reject(new AMQPError("Timeout", this.channel.connection));
                this.timeoutId = setTimeout(onTimeout, timeout);
            }
        });
    }
    cancel() {
        return this.channel.basicCancel(this.tag);
    }
    setClosed(err) {
        this.closed = true;
        if (err)
            this.closedError = err;
        if (this.timeoutId)
            clearTimeout(this.timeoutId);
        if (err) {
            if (this.rejectWait)
                this.rejectWait(err);
        }
        else {
            if (this.resolveWait)
                this.resolveWait();
        }
    }
}

class AMQPChannel {
    constructor(connection, id) {
        this.consumers = new Map();
        this.promises = [];
        this.unconfirmedPublishes = [];
        this.closed = false;
        this.confirmId = 0;
        this.connection = connection;
        this.id = id;
        this.buffer = new AMQPView(new ArrayBuffer(connection.frameMax));
    }
    queue(name = "", { passive = false, durable = name !== "", autoDelete = name === "", exclusive = name === "" } = {}, args = {}) {
        return new Promise((resolve, reject) => {
            this.queueDeclare(name, { passive, durable, autoDelete, exclusive }, args)
                .then(({ name }) => resolve(new AMQPQueue(this, name)))
                .catch(reject);
        });
    }
    prefetch(prefetchCount) {
        return this.basicQos(prefetchCount);
    }
    onReturn(message) {
        console.error("Message returned from server", message);
    }
    close(reason = "", code = 200) {
        if (this.closed)
            return this.rejectClosed();
        this.closed = true;
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 20);
        j += 2;
        frame.setUint16(j, 40);
        j += 2;
        frame.setUint16(j, code);
        j += 2;
        j += frame.setShortString(j, reason);
        frame.setUint16(j, 0);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return this.sendRpc(frame, j);
    }
    basicGet(queue, { noAck = true } = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 11);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 70);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        j += frame.setShortString(j, queue);
        frame.setUint8(j, noAck ? 1 : 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return this.sendRpc(frame, j);
    }
    basicConsume(queue, { tag = "", noAck = true, exclusive = false, args = {} } = {}, callback) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 20);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        j += frame.setShortString(j, queue);
        j += frame.setShortString(j, tag);
        let bits = 0;
        if (noAck)
            bits = bits | (1 << 1);
        if (exclusive)
            bits = bits | (1 << 2);
        frame.setUint8(j, bits);
        j += 1;
        j += frame.setTable(j, args);
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return new Promise((resolve, reject) => {
            this.sendRpc(frame, j).then((consumerTag) => {
                const consumer = new AMQPConsumer(this, consumerTag, callback);
                this.consumers.set(consumerTag, consumer);
                resolve(consumer);
            }).catch(reject);
        });
    }
    basicCancel(tag) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 30);
        j += 2;
        j += frame.setShortString(j, tag);
        frame.setUint8(j, 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return new Promise((resolve, reject) => {
            this.sendRpc(frame, j).then((consumerTag) => {
                const consumer = this.consumers.get(consumerTag);
                if (consumer) {
                    consumer.setClosed();
                    this.consumers.delete(consumerTag);
                }
                resolve(this);
            }).catch(reject);
        });
    }
    basicAck(deliveryTag, multiple = false) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 13);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 80);
        j += 2;
        frame.setUint64(j, deliveryTag);
        j += 8;
        frame.setUint8(j, multiple ? 1 : 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.connection.send(new Uint8Array(frame.buffer, 0, 21));
    }
    basicNack(deliveryTag, requeue = false, multiple = false) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 13);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 120);
        j += 2;
        frame.setUint64(j, deliveryTag);
        j += 8;
        let bits = 0;
        if (multiple)
            bits = bits | (1 << 0);
        if (requeue)
            bits = bits | (1 << 1);
        frame.setUint8(j, bits);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.connection.send(new Uint8Array(frame.buffer, 0, 21));
    }
    basicReject(deliveryTag, requeue = false) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 13);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 90);
        j += 2;
        frame.setUint64(j, deliveryTag);
        j += 8;
        frame.setUint8(j, requeue ? 1 : 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.connection.send(new Uint8Array(frame.buffer, 0, 21));
    }
    basicRecover(requeue = false) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 5);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 110);
        j += 2;
        frame.setUint8(j, requeue ? 1 : 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.sendRpc(frame, j);
    }
    async basicPublish(exchange, routingKey, data, properties = {}, mandatory = false, immediate = false) {
        if (this.closed)
            return this.rejectClosed();
        if (this.connection.blocked)
            return Promise.reject(new AMQPError(`Connection blocked by server: ${this.connection.blocked}`, this.connection));
        let body;
        if (typeof Buffer !== "undefined" && data instanceof Buffer) {
            body = data;
        }
        else if (data instanceof Uint8Array) {
            body = data;
        }
        else if (data instanceof ArrayBuffer) {
            body = new Uint8Array(data);
        }
        else if (data === null) {
            body = new Uint8Array(0);
        }
        else if (typeof data === "string") {
            body = AMQPChannel.textEncoder.encode(data);
        }
        else {
            throw new TypeError(`Invalid type ${typeof data} for parameter data`);
        }
        let j = 0;
        const buffer = this.buffer;
        buffer.setUint8(j, 1);
        j += 1;
        buffer.setUint16(j, this.id);
        j += 2;
        j += 4;
        buffer.setUint16(j, 60);
        j += 2;
        buffer.setUint16(j, 40);
        j += 2;
        buffer.setUint16(j, 0);
        j += 2;
        j += buffer.setShortString(j, exchange);
        j += buffer.setShortString(j, routingKey);
        let bits = 0;
        if (mandatory)
            bits = bits | (1 << 0);
        if (immediate)
            bits = bits | (1 << 1);
        buffer.setUint8(j, bits);
        j += 1;
        buffer.setUint8(j, 206);
        j += 1;
        buffer.setUint32(3, j - 8);
        const headerStart = j;
        buffer.setUint8(j, 2);
        j += 1;
        buffer.setUint16(j, this.id);
        j += 2;
        j += 4;
        buffer.setUint16(j, 60);
        j += 2;
        buffer.setUint16(j, 0);
        j += 2;
        buffer.setUint32(j, 0);
        j += 4;
        buffer.setUint32(j, body.byteLength);
        j += 4;
        j += buffer.setProperties(j, properties);
        buffer.setUint8(j, 206);
        j += 1;
        buffer.setUint32(headerStart + 3, j - headerStart - 8);
        if (body.byteLength === 0) {
            await this.connection.send(new Uint8Array(buffer.buffer, 0, j));
        }
        else if (j >= buffer.byteLength - 8) {
            await this.connection.send(new Uint8Array(buffer.buffer, 0, j));
            j = 0;
        }
        for (let bodyPos = 0; bodyPos < body.byteLength;) {
            const frameSize = Math.min(body.byteLength - bodyPos, buffer.byteLength - 8 - j);
            const dataSlice = body.subarray(bodyPos, bodyPos + frameSize);
            buffer.setUint8(j, 3);
            j += 1;
            buffer.setUint16(j, this.id);
            j += 2;
            buffer.setUint32(j, frameSize);
            j += 4;
            const bodyView = new Uint8Array(buffer.buffer, j, frameSize);
            bodyView.set(dataSlice);
            j += frameSize;
            buffer.setUint8(j, 206);
            j += 1;
            await this.connection.send(new Uint8Array(buffer.buffer, 0, j));
            bodyPos += frameSize;
            j = 0;
        }
        if (this.confirmId) {
            return new Promise((resolve, reject) => this.unconfirmedPublishes.push([this.confirmId++, resolve, reject]));
        }
        else {
            return Promise.resolve(0);
        }
    }
    basicQos(prefetchCount, prefetchSize = 0, global = false) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 11);
        j += 4;
        frame.setUint16(j, 60);
        j += 2;
        frame.setUint16(j, 10);
        j += 2;
        frame.setUint32(j, prefetchSize);
        j += 4;
        frame.setUint16(j, prefetchCount);
        j += 2;
        frame.setUint8(j, global ? 1 : 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.sendRpc(frame, j);
    }
    basicFlow(active = true) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 5);
        j += 4;
        frame.setUint16(j, 20);
        j += 2;
        frame.setUint16(j, 20);
        j += 2;
        frame.setUint8(j, active ? 1 : 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.sendRpc(frame, j);
    }
    confirmSelect() {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 5);
        j += 4;
        frame.setUint16(j, 85);
        j += 2;
        frame.setUint16(j, 10);
        j += 2;
        frame.setUint8(j, 0);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        return this.sendRpc(frame, j);
    }
    queueDeclare(name = "", { passive = false, durable = name !== "", autoDelete = name === "", exclusive = name === "" } = {}, args = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const declare = this.buffer;
        declare.setUint8(j, 1);
        j += 1;
        declare.setUint16(j, this.id);
        j += 2;
        declare.setUint32(j, 0);
        j += 4;
        declare.setUint16(j, 50);
        j += 2;
        declare.setUint16(j, 10);
        j += 2;
        declare.setUint16(j, 0);
        j += 2;
        j += declare.setShortString(j, name);
        let bits = 0;
        if (passive)
            bits = bits | (1 << 0);
        if (durable)
            bits = bits | (1 << 1);
        if (exclusive)
            bits = bits | (1 << 2);
        if (autoDelete)
            bits = bits | (1 << 3);
        declare.setUint8(j, bits);
        j += 1;
        j += declare.setTable(j, args);
        declare.setUint8(j, 206);
        j += 1;
        declare.setUint32(3, j - 8);
        return this.sendRpc(declare, j);
    }
    queueDelete(name = "", { ifUnused = false, ifEmpty = false } = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 50);
        j += 2;
        frame.setUint16(j, 40);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        j += frame.setShortString(j, name);
        let bits = 0;
        if (ifUnused)
            bits = bits | (1 << 0);
        if (ifEmpty)
            bits = bits | (1 << 1);
        frame.setUint8(j, bits);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return this.sendRpc(frame, j);
    }
    queueBind(queue, exchange, routingKey, args = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const bind = this.buffer;
        bind.setUint8(j, 1);
        j += 1;
        bind.setUint16(j, this.id);
        j += 2;
        bind.setUint32(j, 0);
        j += 4;
        bind.setUint16(j, 50);
        j += 2;
        bind.setUint16(j, 20);
        j += 2;
        bind.setUint16(j, 0);
        j += 2;
        j += bind.setShortString(j, queue);
        j += bind.setShortString(j, exchange);
        j += bind.setShortString(j, routingKey);
        bind.setUint8(j, 0);
        j += 1;
        j += bind.setTable(j, args);
        bind.setUint8(j, 206);
        j += 1;
        bind.setUint32(3, j - 8);
        return this.sendRpc(bind, j);
    }
    queueUnbind(queue, exchange, routingKey, args = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const unbind = this.buffer;
        unbind.setUint8(j, 1);
        j += 1;
        unbind.setUint16(j, this.id);
        j += 2;
        unbind.setUint32(j, 0);
        j += 4;
        unbind.setUint16(j, 50);
        j += 2;
        unbind.setUint16(j, 50);
        j += 2;
        unbind.setUint16(j, 0);
        j += 2;
        j += unbind.setShortString(j, queue);
        j += unbind.setShortString(j, exchange);
        j += unbind.setShortString(j, routingKey);
        j += unbind.setTable(j, args);
        unbind.setUint8(j, 206);
        j += 1;
        unbind.setUint32(3, j - 8);
        return this.sendRpc(unbind, j);
    }
    queuePurge(queue) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const purge = this.buffer;
        purge.setUint8(j, 1);
        j += 1;
        purge.setUint16(j, this.id);
        j += 2;
        purge.setUint32(j, 0);
        j += 4;
        purge.setUint16(j, 50);
        j += 2;
        purge.setUint16(j, 30);
        j += 2;
        purge.setUint16(j, 0);
        j += 2;
        j += purge.setShortString(j, queue);
        purge.setUint8(j, 0);
        j += 1;
        purge.setUint8(j, 206);
        j += 1;
        purge.setUint32(3, j - 8);
        return this.sendRpc(purge, j);
    }
    exchangeDeclare(name, type, { passive = false, durable = true, autoDelete = false, internal = false } = {}, args = {}) {
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 40);
        j += 2;
        frame.setUint16(j, 10);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        j += frame.setShortString(j, name);
        j += frame.setShortString(j, type);
        let bits = 0;
        if (passive)
            bits = bits | (1 << 0);
        if (durable)
            bits = bits | (1 << 1);
        if (autoDelete)
            bits = bits | (1 << 2);
        if (internal)
            bits = bits | (1 << 3);
        frame.setUint8(j, bits);
        j += 1;
        j += frame.setTable(j, args);
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return this.sendRpc(frame, j);
    }
    exchangeDelete(name, { ifUnused = false } = {}) {
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 40);
        j += 2;
        frame.setUint16(j, 20);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        j += frame.setShortString(j, name);
        let bits = 0;
        if (ifUnused)
            bits = bits | (1 << 0);
        frame.setUint8(j, bits);
        j += 1;
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return this.sendRpc(frame, j);
    }
    exchangeBind(destination, source, routingKey = "", args = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const bind = this.buffer;
        bind.setUint8(j, 1);
        j += 1;
        bind.setUint16(j, this.id);
        j += 2;
        bind.setUint32(j, 0);
        j += 4;
        bind.setUint16(j, 40);
        j += 2;
        bind.setUint16(j, 30);
        j += 2;
        bind.setUint16(j, 0);
        j += 2;
        j += bind.setShortString(j, destination);
        j += bind.setShortString(j, source);
        j += bind.setShortString(j, routingKey);
        bind.setUint8(j, 0);
        j += 1;
        j += bind.setTable(j, args);
        bind.setUint8(j, 206);
        j += 1;
        bind.setUint32(3, j - 8);
        return this.sendRpc(bind, j);
    }
    exchangeUnbind(destination, source, routingKey = "", args = {}) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const unbind = this.buffer;
        unbind.setUint8(j, 1);
        j += 1;
        unbind.setUint16(j, this.id);
        j += 2;
        unbind.setUint32(j, 0);
        j += 4;
        unbind.setUint16(j, 40);
        j += 2;
        unbind.setUint16(j, 40);
        j += 2;
        unbind.setUint16(j, 0);
        j += 2;
        j += unbind.setShortString(j, destination);
        j += unbind.setShortString(j, source);
        j += unbind.setShortString(j, routingKey);
        unbind.setUint8(j, 0);
        j += 1;
        j += unbind.setTable(j, args);
        unbind.setUint8(j, 206);
        j += 1;
        unbind.setUint32(3, j - 8);
        return this.sendRpc(unbind, j);
    }
    txSelect() {
        return this.txMethod(10);
    }
    txCommit() {
        return this.txMethod(20);
    }
    txRollback() {
        return this.txMethod(30);
    }
    txMethod(methodId) {
        if (this.closed)
            return this.rejectClosed();
        let j = 0;
        const frame = this.buffer;
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, this.id);
        j += 2;
        frame.setUint32(j, 4);
        j += 4;
        frame.setUint16(j, 90);
        j += 2;
        frame.setUint16(j, methodId);
        j += 2;
        frame.setUint8(j, 206);
        j += 1;
        return this.sendRpc(frame, j);
    }
    resolvePromise(value) {
        const promise = this.promises.shift();
        if (promise) {
            const [resolve,] = promise;
            resolve(value);
            return true;
        }
        return false;
    }
    rejectPromise(err) {
        const promise = this.promises.shift();
        if (promise) {
            const [, reject] = promise;
            reject(err);
            return true;
        }
        return false;
    }
    sendRpc(frame, frameSize) {
        return new Promise((resolve, reject) => {
            this.connection.send(new Uint8Array(frame.buffer, 0, frameSize))
                .then(() => this.promises.push([resolve, reject]))
                .catch(reject);
        });
    }
    setClosed(err) {
        if (!this.closed) {
            this.closed = true;
            this.consumers.forEach((consumer) => consumer.setClosed(err));
            this.consumers.clear();
            while (this.rejectPromise(err)) {
            }
            this.unconfirmedPublishes.forEach(([, , reject]) => reject(err));
        }
    }
    rejectClosed() {
        return Promise.reject(new AMQPError("Channel is closed", this.connection));
    }
    publishConfirmed(deliveryTag, multiple, nack) {
        const idx = this.unconfirmedPublishes.findIndex(([tag,]) => tag === deliveryTag);
        if (idx !== -1) {
            const confirmed = multiple ?
                this.unconfirmedPublishes.splice(0, idx + 1) :
                this.unconfirmedPublishes.splice(idx, 1);
            confirmed.forEach(([tag, resolve, reject]) => {
                if (nack)
                    reject(new Error("Message rejected"));
                else
                    resolve(tag);
            });
        }
        else {
            console.warn("Cant find unconfirmed deliveryTag", deliveryTag, "multiple:", multiple, "nack:", nack);
        }
    }
    onMessageReady(message) {
        if (this.delivery) {
            delete this.delivery;
            this.deliver(message);
        }
        else if (this.getMessage) {
            delete this.getMessage;
            this.resolvePromise(message);
        }
        else {
            delete this.returned;
            this.onReturn(message);
        }
    }
    deliver(message) {
        queueMicrotask(() => {
            const consumer = this.consumers.get(message.consumerTag);
            if (consumer) {
                consumer.onMessage(message);
            }
            else {
                console.warn("Consumer", message.consumerTag, "not available on channel", this.id);
            }
        });
    }
}
AMQPChannel.textEncoder = new TextEncoder();

class AMQPMessage {
    constructor(channel) {
        this.exchange = "";
        this.routingKey = "";
        this.properties = {};
        this.bodySize = 0;
        this.bodyPos = 0;
        this.deliveryTag = 0;
        this.consumerTag = "";
        this.redelivered = false;
        this.channel = channel;
    }
    bodyToString() {
        return AMQPMessage.decoder.decode(this.body);
    }
    bodyString() {
        return this.bodyToString();
    }
    ack(multiple = false) {
        return this.channel.basicAck(this.deliveryTag, multiple);
    }
    nack(requeue = false, multiple = false) {
        return this.channel.basicNack(this.deliveryTag, requeue, multiple);
    }
    reject(requeue = false) {
        return this.channel.basicReject(this.deliveryTag, requeue);
    }
    cancelConsumer() {
        return this.channel.basicCancel(this.consumerTag);
    }
}
AMQPMessage.decoder = new TextDecoder();

const VERSION = '2.0.0';
class AMQPBaseClient {
    constructor(vhost, username, password, name, platform, frameMax = 4096, heartbeat = 0) {
        this.closed = false;
        this.channelMax = 0;
        this.vhost = vhost;
        this.username = username;
        this.password = "";
        Object.defineProperty(this, 'password', {
            value: password,
            enumerable: false
        });
        if (name)
            this.name = name;
        if (platform)
            this.platform = platform;
        this.channels = [new AMQPChannel(this, 0)];
        this.closed = false;
        if (frameMax < 4096)
            throw new Error("frameMax must be 4096 or larger");
        this.frameMax = frameMax;
        if (heartbeat < 0)
            throw new Error("heartbeat must be positive");
        this.heartbeat = heartbeat;
    }
    channel(id) {
        if (this.closed)
            return this.rejectClosed();
        if (id && id > 0) {
            const channel = this.channels[id];
            if (channel)
                return Promise.resolve(channel);
        }
        if (!id)
            id = this.channels.findIndex((ch) => ch === undefined);
        if (id === -1)
            id = this.channels.length;
        const channel = new AMQPChannel(this, id);
        this.channels[id] = channel;
        let j = 0;
        const channelOpen = new AMQPView(new ArrayBuffer(13));
        channelOpen.setUint8(j, 1);
        j += 1;
        channelOpen.setUint16(j, id);
        j += 2;
        channelOpen.setUint32(j, 5);
        j += 4;
        channelOpen.setUint16(j, 20);
        j += 2;
        channelOpen.setUint16(j, 10);
        j += 2;
        channelOpen.setUint8(j, 0);
        j += 1;
        channelOpen.setUint8(j, 206);
        j += 1;
        return new Promise((resolve, reject) => {
            this.send(new Uint8Array(channelOpen.buffer, 0, 13))
                .then(() => channel.promises.push([resolve, reject]))
                .catch(reject);
        });
    }
    close(reason = "", code = 200) {
        if (this.closed)
            return this.rejectClosed();
        this.closed = true;
        let j = 0;
        const frame = new AMQPView(new ArrayBuffer(512));
        frame.setUint8(j, 1);
        j += 1;
        frame.setUint16(j, 0);
        j += 2;
        frame.setUint32(j, 0);
        j += 4;
        frame.setUint16(j, 10);
        j += 2;
        frame.setUint16(j, 50);
        j += 2;
        frame.setUint16(j, code);
        j += 2;
        j += frame.setShortString(j, reason);
        frame.setUint16(j, 0);
        j += 2;
        frame.setUint16(j, 0);
        j += 2;
        frame.setUint8(j, 206);
        j += 1;
        frame.setUint32(3, j - 8);
        return new Promise((resolve, reject) => {
            this.send(new Uint8Array(frame.buffer, 0, j))
                .then(() => this.closePromise = [resolve, reject])
                .catch(reject);
        });
    }
    rejectClosed() {
        return Promise.reject(new AMQPError("Connection closed", this));
    }
    rejectConnect(err) {
        if (this.connectPromise) {
            const [, reject] = this.connectPromise;
            delete this.connectPromise;
            reject(err);
        }
        this.closed = true;
        this.closeSocket();
    }
    parseFrames(view) {
        for (let i = 0; i < view.byteLength;) {
            let j = 0;
            const type = view.getUint8(i);
            i += 1;
            const channelId = view.getUint16(i);
            i += 2;
            const frameSize = view.getUint32(i);
            i += 4;
            try {
                const frameEnd = view.getUint8(i + frameSize);
                if (frameEnd !== 206)
                    throw (new AMQPError(`Invalid frame end ${frameEnd}, expected 206`, this));
            }
            catch (e) {
                throw (new AMQPError(`Frame end out of range, frameSize=${frameSize}, pos=${i}, byteLength=${view.byteLength}`, this));
            }
            const channel = this.channels[channelId];
            if (!channel) {
                console.warn("AMQP channel", channelId, "not open");
                i += frameSize + 1;
                continue;
            }
            switch (type) {
                case 1: {
                    const classId = view.getUint16(i);
                    i += 2;
                    const methodId = view.getUint16(i);
                    i += 2;
                    switch (classId) {
                        case 10: {
                            switch (methodId) {
                                case 10: {
                                    i += frameSize - 4;
                                    const startOk = new AMQPView(new ArrayBuffer(4096));
                                    startOk.setUint8(j, 1);
                                    j += 1;
                                    startOk.setUint16(j, 0);
                                    j += 2;
                                    startOk.setUint32(j, 0);
                                    j += 4;
                                    startOk.setUint16(j, 10);
                                    j += 2;
                                    startOk.setUint16(j, 11);
                                    j += 2;
                                    const clientProps = {
                                        connection_name: this.name || undefined,
                                        product: "amqp-client.js",
                                        information: "https://github.com/cloudamqp/amqp-client.js",
                                        version: VERSION,
                                        platform: this.platform,
                                        capabilities: {
                                            "authentication_failure_close": true,
                                            "basic.nack": true,
                                            "connection.blocked": true,
                                            "consumer_cancel_notify": true,
                                            "exchange_exchange_bindings": true,
                                            "per_consumer_qos": true,
                                            "publisher_confirms": true,
                                        }
                                    };
                                    j += startOk.setTable(j, clientProps);
                                    j += startOk.setShortString(j, "PLAIN");
                                    const response = `\u0000${this.username}\u0000${this.password}`;
                                    j += startOk.setLongString(j, response);
                                    j += startOk.setShortString(j, "");
                                    startOk.setUint8(j, 206);
                                    j += 1;
                                    startOk.setUint32(3, j - 8);
                                    this.send(new Uint8Array(startOk.buffer, 0, j)).catch(this.rejectConnect);
                                    break;
                                }
                                case 30: {
                                    const channelMax = view.getUint16(i);
                                    i += 2;
                                    const frameMax = view.getUint32(i);
                                    i += 4;
                                    const heartbeat = view.getUint16(i);
                                    i += 2;
                                    this.channelMax = channelMax;
                                    this.frameMax = this.frameMax === 0 ? frameMax : Math.min(this.frameMax, frameMax);
                                    this.heartbeat = this.heartbeat === 0 ? 0 : Math.min(this.heartbeat, heartbeat);
                                    const tuneOk = new AMQPView(new ArrayBuffer(20));
                                    tuneOk.setUint8(j, 1);
                                    j += 1;
                                    tuneOk.setUint16(j, 0);
                                    j += 2;
                                    tuneOk.setUint32(j, 12);
                                    j += 4;
                                    tuneOk.setUint16(j, 10);
                                    j += 2;
                                    tuneOk.setUint16(j, 31);
                                    j += 2;
                                    tuneOk.setUint16(j, this.channelMax);
                                    j += 2;
                                    tuneOk.setUint32(j, this.frameMax);
                                    j += 4;
                                    tuneOk.setUint16(j, this.heartbeat);
                                    j += 2;
                                    tuneOk.setUint8(j, 206);
                                    j += 1;
                                    this.send(new Uint8Array(tuneOk.buffer, 0, j)).catch(this.rejectConnect);
                                    j = 0;
                                    const open = new AMQPView(new ArrayBuffer(512));
                                    open.setUint8(j, 1);
                                    j += 1;
                                    open.setUint16(j, 0);
                                    j += 2;
                                    open.setUint32(j, 0);
                                    j += 4;
                                    open.setUint16(j, 10);
                                    j += 2;
                                    open.setUint16(j, 40);
                                    j += 2;
                                    j += open.setShortString(j, this.vhost);
                                    open.setUint8(j, 0);
                                    j += 1;
                                    open.setUint8(j, 0);
                                    j += 1;
                                    open.setUint8(j, 206);
                                    j += 1;
                                    open.setUint32(3, j - 8);
                                    this.send(new Uint8Array(open.buffer, 0, j)).catch(this.rejectConnect);
                                    break;
                                }
                                case 41: {
                                    i += 1;
                                    const promise = this.connectPromise;
                                    if (promise) {
                                        const [resolve,] = promise;
                                        delete this.connectPromise;
                                        resolve(this);
                                    }
                                    break;
                                }
                                case 50: {
                                    const code = view.getUint16(i);
                                    i += 2;
                                    const [text, strLen] = view.getShortString(i);
                                    i += strLen;
                                    const classId = view.getUint16(i);
                                    i += 2;
                                    const methodId = view.getUint16(i);
                                    i += 2;
                                    console.debug("connection closed by server", code, text, classId, methodId);
                                    const msg = `connection closed: ${text} (${code})`;
                                    const err = new AMQPError(msg, this);
                                    this.channels.forEach((ch) => ch.setClosed(err));
                                    this.channels = [new AMQPChannel(this, 0)];
                                    const closeOk = new AMQPView(new ArrayBuffer(12));
                                    closeOk.setUint8(j, 1);
                                    j += 1;
                                    closeOk.setUint16(j, 0);
                                    j += 2;
                                    closeOk.setUint32(j, 4);
                                    j += 4;
                                    closeOk.setUint16(j, 10);
                                    j += 2;
                                    closeOk.setUint16(j, 51);
                                    j += 2;
                                    closeOk.setUint8(j, 206);
                                    j += 1;
                                    this.send(new Uint8Array(closeOk.buffer, 0, j))
                                        .catch(err => console.warn("Error while sending Connection#CloseOk", err));
                                    this.rejectConnect(err);
                                    break;
                                }
                                case 51: {
                                    this.channels.forEach((ch) => ch.setClosed());
                                    this.channels = [new AMQPChannel(this, 0)];
                                    const promise = this.closePromise;
                                    if (promise) {
                                        const [resolve,] = promise;
                                        delete this.closePromise;
                                        resolve();
                                        this.closeSocket();
                                    }
                                    break;
                                }
                                case 60: {
                                    const [reason, len] = view.getShortString(i);
                                    i += len;
                                    console.warn("AMQP connection blocked:", reason);
                                    this.blocked = reason;
                                    break;
                                }
                                case 61: {
                                    console.info("AMQP connection unblocked");
                                    delete this.blocked;
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        case 20: {
                            switch (methodId) {
                                case 11: {
                                    i += 4;
                                    channel.resolvePromise(channel);
                                    break;
                                }
                                case 21: {
                                    const active = view.getUint8(i) !== 0;
                                    i += 1;
                                    channel.resolvePromise(active);
                                    break;
                                }
                                case 40: {
                                    const code = view.getUint16(i);
                                    i += 2;
                                    const [text, strLen] = view.getShortString(i);
                                    i += strLen;
                                    const classId = view.getUint16(i);
                                    i += 2;
                                    const methodId = view.getUint16(i);
                                    i += 2;
                                    console.debug("channel", channelId, "closed", code, text, classId, methodId);
                                    const msg = `channel ${channelId} closed: ${text} (${code})`;
                                    const err = new AMQPError(msg, this);
                                    channel.setClosed(err);
                                    delete this.channels[channelId];
                                    const closeOk = new AMQPView(new ArrayBuffer(12));
                                    closeOk.setUint8(j, 1);
                                    j += 1;
                                    closeOk.setUint16(j, channelId);
                                    j += 2;
                                    closeOk.setUint32(j, 4);
                                    j += 4;
                                    closeOk.setUint16(j, 20);
                                    j += 2;
                                    closeOk.setUint16(j, 41);
                                    j += 2;
                                    closeOk.setUint8(j, 206);
                                    j += 1;
                                    this.send(new Uint8Array(closeOk.buffer, 0, j))
                                        .catch(err => console.error("Error while sending Channel#closeOk", err));
                                    break;
                                }
                                case 41: {
                                    channel.setClosed();
                                    delete this.channels[channelId];
                                    channel.resolvePromise();
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        case 40: {
                            switch (methodId) {
                                case 11:
                                case 21:
                                case 31:
                                case 51: {
                                    channel.resolvePromise();
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        case 50: {
                            switch (methodId) {
                                case 11: {
                                    const [name, strLen] = view.getShortString(i);
                                    i += strLen;
                                    const messageCount = view.getUint32(i);
                                    i += 4;
                                    const consumerCount = view.getUint32(i);
                                    i += 4;
                                    channel.resolvePromise({ name, messageCount, consumerCount });
                                    break;
                                }
                                case 21: {
                                    channel.resolvePromise();
                                    break;
                                }
                                case 31: {
                                    const messageCount = view.getUint32(i);
                                    i += 4;
                                    channel.resolvePromise({ messageCount });
                                    break;
                                }
                                case 41: {
                                    const messageCount = view.getUint32(i);
                                    i += 4;
                                    channel.resolvePromise({ messageCount });
                                    break;
                                }
                                case 51: {
                                    channel.resolvePromise();
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        case 60: {
                            switch (methodId) {
                                case 11: {
                                    channel.resolvePromise();
                                    break;
                                }
                                case 21: {
                                    const [consumerTag, len] = view.getShortString(i);
                                    i += len;
                                    channel.resolvePromise(consumerTag);
                                    break;
                                }
                                case 30: {
                                    const [consumerTag, len] = view.getShortString(i);
                                    i += len;
                                    const noWait = view.getUint8(i) === 1;
                                    i += 1;
                                    const consumer = channel.consumers.get(consumerTag);
                                    if (consumer) {
                                        consumer.setClosed(new AMQPError("Consumer cancelled by the server", this));
                                        channel.consumers.delete(consumerTag);
                                    }
                                    if (!noWait) {
                                        const frame = new AMQPView(new ArrayBuffer(512));
                                        frame.setUint8(j, 1);
                                        j += 1;
                                        frame.setUint16(j, channel.id);
                                        j += 2;
                                        frame.setUint32(j, 0);
                                        j += 4;
                                        frame.setUint16(j, 60);
                                        j += 2;
                                        frame.setUint16(j, 31);
                                        j += 2;
                                        j += frame.setShortString(j, consumerTag);
                                        frame.setUint8(j, 206);
                                        j += 1;
                                        frame.setUint32(3, j - 8);
                                        this.send(new Uint8Array(frame.buffer, 0, j));
                                    }
                                    break;
                                }
                                case 31: {
                                    const [consumerTag, len] = view.getShortString(i);
                                    i += len;
                                    channel.resolvePromise(consumerTag);
                                    break;
                                }
                                case 50: {
                                    const code = view.getUint16(i);
                                    i += 2;
                                    const [text, len] = view.getShortString(i);
                                    i += len;
                                    const [exchange, exchangeLen] = view.getShortString(i);
                                    i += exchangeLen;
                                    const [routingKey, routingKeyLen] = view.getShortString(i);
                                    i += routingKeyLen;
                                    const message = new AMQPMessage(channel);
                                    message.exchange = exchange;
                                    message.routingKey = routingKey;
                                    message.replyCode = code;
                                    message.replyText = text;
                                    channel.returned = message;
                                    break;
                                }
                                case 60: {
                                    const [consumerTag, consumerTagLen] = view.getShortString(i);
                                    i += consumerTagLen;
                                    const deliveryTag = view.getUint64(i);
                                    i += 8;
                                    const redelivered = view.getUint8(i) === 1;
                                    i += 1;
                                    const [exchange, exchangeLen] = view.getShortString(i);
                                    i += exchangeLen;
                                    const [routingKey, routingKeyLen] = view.getShortString(i);
                                    i += routingKeyLen;
                                    const message = new AMQPMessage(channel);
                                    message.consumerTag = consumerTag;
                                    message.deliveryTag = deliveryTag;
                                    message.exchange = exchange;
                                    message.routingKey = routingKey;
                                    message.redelivered = redelivered;
                                    channel.delivery = message;
                                    break;
                                }
                                case 71: {
                                    const deliveryTag = view.getUint64(i);
                                    i += 8;
                                    const redelivered = view.getUint8(i) === 1;
                                    i += 1;
                                    const [exchange, exchangeLen] = view.getShortString(i);
                                    i += exchangeLen;
                                    const [routingKey, routingKeyLen] = view.getShortString(i);
                                    i += routingKeyLen;
                                    const messageCount = view.getUint32(i);
                                    i += 4;
                                    const message = new AMQPMessage(channel);
                                    message.deliveryTag = deliveryTag;
                                    message.redelivered = redelivered;
                                    message.exchange = exchange;
                                    message.routingKey = routingKey;
                                    message.messageCount = messageCount;
                                    channel.getMessage = message;
                                    break;
                                }
                                case 72: {
                                    const [, len] = view.getShortString(i);
                                    i += len;
                                    channel.resolvePromise(null);
                                    break;
                                }
                                case 80: {
                                    const deliveryTag = view.getUint64(i);
                                    i += 8;
                                    const multiple = view.getUint8(i) === 1;
                                    i += 1;
                                    channel.publishConfirmed(deliveryTag, multiple, false);
                                    break;
                                }
                                case 111: {
                                    channel.resolvePromise();
                                    break;
                                }
                                case 120: {
                                    const deliveryTag = view.getUint64(i);
                                    i += 8;
                                    const multiple = view.getUint8(i) === 1;
                                    i += 1;
                                    channel.publishConfirmed(deliveryTag, multiple, true);
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        case 85: {
                            switch (methodId) {
                                case 11: {
                                    channel.confirmId = 1;
                                    channel.resolvePromise();
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        case 90: {
                            switch (methodId) {
                                case 11:
                                case 21:
                                case 31: {
                                    channel.resolvePromise();
                                    break;
                                }
                                default:
                                    i += frameSize - 4;
                                    console.error("unsupported class/method id", classId, methodId);
                            }
                            break;
                        }
                        default:
                            i += frameSize - 2;
                            console.error("unsupported class id", classId);
                    }
                    break;
                }
                case 2: {
                    i += 4;
                    const bodySize = view.getUint64(i);
                    i += 8;
                    const [properties, propLen] = view.getProperties(i);
                    i += propLen;
                    const message = channel.delivery || channel.getMessage || channel.returned;
                    if (message) {
                        message.bodySize = bodySize;
                        message.properties = properties;
                        message.body = new Uint8Array(bodySize);
                        if (bodySize === 0)
                            channel.onMessageReady(message);
                    }
                    else {
                        console.warn("Header frame but no message");
                    }
                    break;
                }
                case 3: {
                    const message = channel.delivery || channel.getMessage || channel.returned;
                    if (message && message.body) {
                        const bodyPart = new Uint8Array(view.buffer, view.byteOffset + i, frameSize);
                        message.body.set(bodyPart, message.bodyPos);
                        message.bodyPos += frameSize;
                        i += frameSize;
                        if (message.bodyPos === message.bodySize)
                            channel.onMessageReady(message);
                    }
                    else {
                        console.warn("Body frame but no message");
                    }
                    break;
                }
                case 8: {
                    const heartbeat = new Uint8Array([1, 0, 0, 0, 0, 0, 0, 206]);
                    this.send(heartbeat).catch(err => console.warn("Error while sending heartbeat", err));
                    break;
                }
                default:
                    console.error("invalid frame type:", type);
                    i += frameSize;
            }
            i += 1;
        }
    }
}

class AMQPWebSocketClient extends AMQPBaseClient {
    constructor(url, vhost = "/", username = "guest", password = "guest", name, frameMax = 4096, heartbeat = 0) {
        super(vhost, username, password, name, AMQPWebSocketClient.platform(), frameMax, heartbeat);
        this.framePos = 0;
        this.frameSize = 0;
        this.url = url;
        this.frameBuffer = new Uint8Array(frameMax);
    }
    connect() {
        const socket = new WebSocket(this.url);
        this.socket = socket;
        socket.binaryType = "arraybuffer";
        socket.onmessage = this.handleMessage.bind(this);
        return new Promise((resolve, reject) => {
            this.connectPromise = [resolve, reject];
            socket.onclose = reject;
            socket.onerror = reject;
            socket.onopen = () => socket.send(new Uint8Array([65, 77, 81, 80, 0, 0, 9, 1]));
        });
    }
    send(bytes) {
        return new Promise((resolve, reject) => {
            if (this.socket) {
                try {
                    this.socket.send(bytes);
                    resolve();
                }
                catch (err) {
                    reject(err);
                }
            }
            else {
                reject("Socket not connected");
            }
        });
    }
    closeSocket() {
        if (this.socket)
            this.socket.close();
    }
    handleMessage(event) {
        const buf = event.data;
        const bufView = new DataView(buf);
        let bufPos = 0;
        while (bufPos < buf.byteLength) {
            if (this.frameSize === 0) {
                if (this.framePos !== 0) {
                    const len = buf.byteLength - bufPos;
                    this.frameBuffer.set(new Uint8Array(buf, bufPos), this.framePos);
                    this.frameSize = new DataView(this.frameBuffer).getInt32(bufPos + 3) + 8;
                    this.framePos += len;
                    bufPos += len;
                    continue;
                }
                if (bufPos + 3 + 4 > buf.byteLength) {
                    const len = buf.byteLength - bufPos;
                    this.frameBuffer.set(new Uint8Array(buf, bufPos), this.framePos);
                    this.framePos += len;
                    break;
                }
                this.frameSize = bufView.getInt32(bufPos + 3) + 8;
                if (buf.byteLength - bufPos >= this.frameSize) {
                    const view = new AMQPView(buf, bufPos, this.frameSize);
                    this.parseFrames(view);
                    bufPos += this.frameSize;
                    this.frameSize = 0;
                    continue;
                }
            }
            const leftOfFrame = this.frameSize - this.framePos;
            const copyBytes = Math.min(leftOfFrame, buf.byteLength - bufPos);
            this.frameBuffer.set(new Uint8Array(buf, bufPos, copyBytes), this.framePos);
            this.framePos += copyBytes;
            bufPos += copyBytes;
            if (this.framePos === this.frameSize) {
                const view = new AMQPView(this.frameBuffer.buffer, 0, this.frameSize);
                this.parseFrames(view);
                this.frameSize = this.framePos = 0;
            }
        }
    }
    static platform() {
        if (typeof (window) !== 'undefined')
            return window.navigator.userAgent;
        else
            return `${process.release.name} ${process.version} ${process.platform} ${process.arch}`;
    }
}

export { AMQPWebSocketClient };
//# sourceMappingURL=amqp-websocket-client.mjs.map
