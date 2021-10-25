FROM node:16 AS docbuilder

WORKDIR /tmp

COPY openapi ./openapi
COPY build ./build
COPY shard.yml package.json package-lock.json ./
RUN npm config set unsafe-perm true && npm ci

FROM 84codes/crystal:1.1.1-ubuntu-20.04 AS builder

# install dependencies
RUN apt-get update && apt-get install -y libsystemd-dev

WORKDIR /tmp

# Copying and install dependencies
COPY shard.yml shard.lock ./
RUN shards install --production

# Copying the rest of the code
COPY ./static ./static
COPY --from=docbuilder /tmp/static/docs/index.html ./static/docs/index.html
COPY ./src ./src

# Build
RUN shards build --production --release

# start from scratch and only copy the built binary
FROM ubuntu:20.04
EXPOSE 5672 15672
VOLUME /data
WORKDIR /data

RUN apt-get update && \
    apt-get install -y libgc1c2 libssl1.1 libevent-2.1-* && \
    apt-get clean && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*

COPY --from=builder /tmp/bin/* /usr/bin/
ENTRYPOINT ["/usr/bin/avalanchemq", "-b", "0.0.0.0", "-D", "/data"]
