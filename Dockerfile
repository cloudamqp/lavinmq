FROM node:10 AS docbuilder

WORKDIR /tmp

COPY openapi ./openapi
COPY build ./build
COPY package.json package-lock.json ./
RUN npm install --unsafe-perm

FROM crystallang/crystal:0.35.1 AS builder

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
RUN shards build --production --release avalanchemq
RUN strip bin/avalanchemq

# start from scratch and only copy the built binary
FROM ubuntu:18.04

LABEL org.opencontainers.image.title='AvalancheMQ'
LABEL org.opencontainers.image.url='https://www.avalanchemq.com/'
LABEL org.opencontainers.image.documentation='https://www.avalanchemq.com/docs'
LABEL org.opencontainers.image.source='https://github.com/cloudamqp/avalanchemq'
LABEL org.opencontainers.image.description='A very fast and lean AMQP server'
LABEL org.opencontainers.image.licenses='Apache-2.0'
EXPOSE 5672 15672
VOLUME /data
WORKDIR /data

RUN apt-get update && \
    apt-get install -y libssl1.1 libevent-2.1-* && \
    apt-get clean && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*

COPY --from=builder /tmp/bin/avalanchemq /usr/bin/
ENTRYPOINT ["/usr/bin/avalanchemq", "-b", "0.0.0.0", "-D", "/data"]
