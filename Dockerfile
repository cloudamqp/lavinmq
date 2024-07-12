# Base layer
FROM 84codes/crystal:1.13.1-ubuntu-24.04 AS base
RUN apt-get update && apt-get install liblz4-dev
WORKDIR /tmp
COPY shard.yml shard.lock .
RUN shards install --production
COPY ./static ./static
COPY ./views ./views
COPY ./src ./src

# Run specs on build platform
FROM base AS spec
RUN apt-get install etcd-server
COPY ./spec ./spec
ARG spec_args="--order random"
RUN crystal spec ${spec_args}

# Lint in another layer
FROM base AS lint
RUN shards install # install ameba only in this layer
COPY .ameba.yml .
RUN bin/ameba
RUN crystal tool format --check

# Build
FROM base AS builder
COPY Makefile .
RUN make js lib
ARG MAKEFLAGS=-j2
RUN make all bin/lavinmq-debug

# Resulting image with minimal layers
FROM ubuntu:24.04
RUN apt-get update && \
    apt-get install -y libssl3 libevent-2.1-7 libevent-pthreads-2.1-7 ca-certificates liblz4-1 && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*
COPY --from=builder /tmp/bin/* /usr/bin/
EXPOSE 5672 15672
VOLUME /var/lib/lavinmq
WORKDIR /var/lib/lavinmq
ENV GC_UNMAP_THRESHOLD=1
ENV CRYSTAL_LOAD_DEBUG_INFO=1
HEALTHCHECK CMD ["/usr/bin/lavinmqctl", "status"]
ENTRYPOINT ["/usr/bin/lavinmq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
