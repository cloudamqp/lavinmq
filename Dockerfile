FROM --platform=$BUILDPLATFORM node:16 AS docbuilder
WORKDIR /tmp

COPY openapi ./openapi
COPY build ./build
COPY shard.yml package.json package-lock.json ./
RUN npm config set unsafe-perm true && npm ci

FROM --platform=$BUILDPLATFORM 84codes/crystal:1.3.2-debian-11 AS builder
WORKDIR /tmp

# Copying and install dependencies
COPY shard.yml shard.lock ./
RUN shards install --production

# Copying the rest of the code
COPY ./static ./static
COPY --from=docbuilder /tmp/static/docs/index.html ./static/docs/index.html
COPY ./src ./src

# Build
ARG TARGETOS TARGETARCH
RUN echo "avalanchemq avalanchemqctl avalanchemqperf" | xargs -d" " -P2 -I{} sh -c "crystal build src/{}.cr --release --no-debug --cross-compile --target $TARGETARCH-unknown-$TARGETOS-gnu > {}.sh"

FROM debian:11-slim as target-builder
WORKDIR /tmp
RUN apt-get update && apt-get install -y build-essential pkg-config libpcre3-dev libevent-dev libssl-dev zlib1g-dev libgc-dev

COPY --from=builder /tmp/*.o /tmp/*.sh .
RUN sh -ex avalanchemq.sh && sh -ex avalanchemqctl.sh && sh -ex avalanchemqperf.sh

# start from scratch and only copy the built binary
FROM debian:11-slim
RUN apt-get update && \
    apt-get install -y libssl1.1 libgc1 libevent-2.1-7 && \
    apt-get clean && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*

COPY --from=target-builder /tmp/avalanchemq /tmp/avalanchemqctl /tmp/avalanchemqperf /usr/bin/

EXPOSE 5672 15672
VOLUME /var/lib/avalanchemq
WORKDIR /var/lib/avalanchemq

HEALTHCHECK CMD /usr/bin/avalanchemqctl status
ENV GC_UNMAP_THRESHOLD=1
ENTRYPOINT ["/usr/bin/avalanchemq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
