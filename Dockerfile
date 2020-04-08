FROM crystallang/crystal:0.34.0 AS builder

WORKDIR /tmp

# Copying and install dependencies
COPY shard.yml shard.lock ./
RUN shards install --production

# Copying the rest of the code
COPY ./static ./static
COPY ./src ./src

# Build
RUN shards build --production --release avalanchemq

# start from scratch and only copy the built binary
FROM ubuntu:18.04
RUN mkdir /data && chown daemon:daemon /data
RUN apt-get update && \
    apt-get install -y libssl1.1 libevent-2.1-* && \
    apt-get clean && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/*
COPY --from=builder /tmp/bin/avalanchemq /usr/sbin/
VOLUME ["/data"]
EXPOSE 15672 5672
USER daemon:daemon
ENTRYPOINT ["/usr/sbin/avalanchemq", "-D", "/data"]
