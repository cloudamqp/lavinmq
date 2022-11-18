# Base layer
FROM --platform=$BUILDPLATFORM 84codes/crystal:1.6.2-ubuntu-22.04 AS base
WORKDIR /tmp
COPY shard.yml shard.lock .
RUN shards install
COPY ./static ./static
COPY ./src ./src

# Run specs on build platform
FROM --platform=$BUILDPLATFORM base AS test
COPY ./spec ./spec
RUN crystal spec --order random
COPY .ameba.yml .
RUN bin/ameba
RUN crystal tool format --check

# Build docs in npm container
FROM --platform=$BUILDPLATFORM node:lts AS docbuilder
WORKDIR /tmp
RUN npm install -g redoc-cli
COPY Makefile shard.yml .
COPY openapi openapi
RUN make docs

# Build objects file on build platform for speed
FROM --platform=$BUILDPLATFORM base AS builder
COPY Makefile .
RUN make js lib
COPY --from=docbuilder /tmp/openapi/openapi.yaml openapi/openapi.yaml
COPY --from=docbuilder /tmp/static/docs/index.html static/docs/index.html
ARG TARGETARCH
RUN make objects target=$TARGETARCH-unknown-linux-gnu -j2

# Link object files on target platform
FROM 84codes/crystal:1.6.2-ubuntu-22.04 AS target-builder
WORKDIR /tmp
COPY Makefile .
COPY --from=builder /tmp/bin bin
RUN make all -j && rm bin/*.*

# Resulting image with minimal layers
FROM ubuntu:22.04
RUN apt-get update && \
    apt-get install -y libssl3 libevent-2.1-7 ca-certificates && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*
COPY --from=target-builder /tmp/bin/* /usr/bin/
EXPOSE 5672 15672
VOLUME /var/lib/lavinmq
WORKDIR /var/lib/lavinmq
ENV GC_UNMAP_THRESHOLD=1
HEALTHCHECK CMD ["/usr/bin/lavinmqctl", "status"]
ENTRYPOINT ["/usr/bin/lavinmq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
