FROM node:16 AS docbuilder

WORKDIR /tmp

COPY openapi ./openapi
COPY build ./build
COPY shard.yml package.json package-lock.json ./
RUN npm config set unsafe-perm true && npm ci

FROM 84codes/crystal:1.3.1-debian-11 AS builder

WORKDIR /tmp

# Copying and install dependencies
COPY shard.yml shard.lock ./
RUN shards install --production

# Copying the rest of the code
COPY ./static ./static
COPY --from=docbuilder /tmp/static/docs/index.html ./static/docs/index.html
COPY ./src ./src

# Build
RUN shards build --production --release --no-debug

# start from scratch and only copy the built binary
FROM gcr.io/distroless/base-debian11
RUN apt-get update && \
    apt-get install -y libssl1.1 libevent-2.1-* && \
    apt-get clean && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*

COPY --from=builder /tmp/bin/* /usr/bin/

EXPOSE 5672 15672
VOLUME /var/lib/avalanchemq
WORKDIR /var/lib/avalanchemq

HEALTHCHECK CMD /usr/bin/avalanchemqctl status
ENV GC_UNMAP_THRESHOLD=1
ENTRYPOINT ["/usr/bin/avalanchemq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
