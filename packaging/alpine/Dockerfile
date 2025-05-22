FROM 84codes/crystal:latest-alpine AS builder
RUN apk add lz4-dev
WORKDIR /tmp
COPY shard.yml shard.lock .
RUN shards install --production
COPY ./static ./static
COPY ./views ./views
COPY ./src ./src
COPY Makefile .
ARG MAKEFLAGS=-j2
RUN make all

FROM alpine:latest
RUN apk add openssl lz4-libs pcre2 libgcc
COPY --from=builder /tmp/bin/* /usr/bin/
EXPOSE 5672 15672
VOLUME /var/lib/lavinmq
WORKDIR /var/lib/lavinmq
ENV GC_UNMAP_THRESHOLD=1
HEALTHCHECK CMD ["/usr/bin/lavinmqctl", "status"]
ENTRYPOINT ["/usr/bin/lavinmq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
