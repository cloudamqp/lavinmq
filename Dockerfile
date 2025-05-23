# Base layer
FROM 84codes/crystal:latest-debian-12 AS base
RUN apt-get update && apt-get install -y liblz4-dev dpkg-dev
WORKDIR /usr/src/lavinmq
COPY shard.yml shard.lock .
RUN shards install --production
COPY ./static ./static
COPY ./views ./views
COPY ./src ./src

# Run specs on build platform
FROM base AS spec
RUN apt-get install -y etcd-server
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
RUN make all
# Use ldd to find all shared libraries and copy them maintaining their original paths
RUN for lib in $(ldd bin/* | grep "=> /" | awk '{print $3}' | sort -u | grep -E "lib(lz4|pcre2|z.so)" ); do \
    mkdir -p /tmp/deps$(dirname $lib); \
    cp -L $lib /tmp/deps$(dirname $lib)/; \
    done

# Resulting image with minimal layers
FROM gcr.io/distroless/cc-debian12:debug
COPY --from=builder /usr/src/lavinmq/bin/* /usr/bin/
COPY --from=builder /tmp/deps/ /
EXPOSE 5672 15672
VOLUME /var/lib/lavinmq
WORKDIR /var/lib/lavinmq
ENV GC_UNMAP_THRESHOLD=1
ENV CRYSTAL_LOAD_DEBUG_INFO=1
HEALTHCHECK CMD ["/usr/bin/lavinmqctl", "status"]
ENTRYPOINT ["/usr/bin/lavinmq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
