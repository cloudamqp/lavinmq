# Base layer
FROM 84codes/crystal:1.6.2-ubuntu-22.04 AS base
WORKDIR /tmp
COPY shard.yml shard.lock .
RUN shards install --production
COPY ./static ./static
COPY ./src ./src

# Run specs on build platform
FROM base AS spec
COPY ./spec ./spec
RUN crystal spec --order random

# Lint in another layer
FROM base AS lint
RUN shards install # install ameba only in this layer
COPY .ameba.yml .
RUN bin/ameba
RUN crystal tool format --check

# Build docs in npm container
FROM node:lts AS docbuilder
WORKDIR /tmp
RUN npm install -g redoc-cli
RUN npm install -g @stoplight/spectral-cli
COPY Makefile shard.yml .
COPY openapi openapi
RUN make docs

# Build
FROM base AS builder
COPY Makefile .
RUN make js lib
COPY --from=docbuilder /tmp/openapi/openapi.yaml /tmp/openapi/.spectral.json openapi/
COPY --from=docbuilder /tmp/static/docs/index.html static/docs/index.html
ARG MAKEFLAGS=-j2
RUN make

# Resulting image with minimal layers
FROM ubuntu:22.04
RUN apt-get update && \
    apt-get install -y libssl3 libevent-2.1-7 ca-certificates && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*
COPY --from=builder /tmp/bin/* /usr/bin/
EXPOSE 5672 15672
VOLUME /var/lib/lavinmq
WORKDIR /var/lib/lavinmq
ENV GC_UNMAP_THRESHOLD=1
HEALTHCHECK CMD ["/usr/bin/lavinmqctl", "status"]
ENTRYPOINT ["/usr/bin/lavinmq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
