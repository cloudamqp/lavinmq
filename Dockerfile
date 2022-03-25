# Build docs in npm container
FROM --platform=$BUILDPLATFORM node:lts AS docbuilder
WORKDIR /tmp
RUN npm install -g redoc-cli
COPY shard.yml .
COPY openapi openapi
RUN redoc-cli bundle openapi/openapi.yaml

# Build objects file on build platform for speed
FROM --platform=$BUILDPLATFORM 84codes/crystal:1.3.2-debian-11 AS builder
RUN apt-get update && apt-get install -y make curl
WORKDIR /tmp
COPY Makefile shard.yml shard.lock .
RUN make js lib
COPY --from=docbuilder /tmp/openapi/openapi.yaml openapi/openapi.yaml
COPY --from=docbuilder /tmp/redoc-static.html static/docs/index.html
COPY ./static ./static
COPY ./src ./src
ARG TARGETARCH
RUN make objects target=$TARGETARCH-unknown-linux-gnu -j2

# Link object files on target platform
FROM 84codes/crystal:1.3.2-debian-11 AS target-builder
WORKDIR /tmp
COPY Makefile .
COPY --from=builder /tmp/bin bin
RUN make all -j && rm bin/*.*

# Resulting image with minimal layers
FROM debian:11-slim
RUN apt-get update && \
    apt-get install -y libssl1.1 libevent-2.1-7 && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/* /var/log/*
COPY --from=target-builder /tmp/bin/* /usr/bin/
EXPOSE 5672 15672
VOLUME /var/lib/avalanchemq
WORKDIR /var/lib/avalanchemq
ENV GC_UNMAP_THRESHOLD=1
HEALTHCHECK CMD ["/usr/bin/avalanchemqctl", "status"]
ENTRYPOINT ["/usr/bin/avalanchemq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
