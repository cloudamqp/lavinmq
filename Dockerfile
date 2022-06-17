# Build docs in npm container
FROM --platform=$BUILDPLATFORM node:lts AS docbuilder
WORKDIR /tmp
RUN npm install -g redoc-cli
COPY Makefile shard.yml .
COPY openapi openapi
RUN make docs

# Build objects file on build platform for speed
FROM --platform=$BUILDPLATFORM 84codes/crystal:1.4.1-ubuntu-22.04 AS builder
RUN apt-get update && apt-get install -y make curl
WORKDIR /tmp
COPY Makefile shard.yml shard.lock .
RUN make js lib
COPY --from=docbuilder /tmp/openapi/openapi.yaml openapi/openapi.yaml
COPY --from=docbuilder /tmp/static/docs/index.html static/docs/index.html
COPY ./static ./static
COPY ./src ./src
ARG TARGETARCH
RUN make objects target=$TARGETARCH-unknown-linux-gnu -j2

# Link object files on target platform
FROM 84codes/crystal:1.4.1-ubuntu-22.04 AS target-builder
WORKDIR /tmp
COPY Makefile .
COPY --from=builder /tmp/bin bin
RUN make all -j && rm bin/*.*
RUN ldd bin/* | awk 'match($0, /(\/.*) /) {print substr($0, RSTART, RLENGTH)}' | sort -u | xargs -I% sh -c 'mkdir -p $(dirname deps%); cp % deps%;'

# Resulting image with minimal layers
FROM scratch
COPY --from=target-builder /tmp/deps/ /
COPY --from=target-builder /tmp/bin/* /usr/bin/
EXPOSE 5672 15672
VOLUME /var/lib/lavinmq
WORKDIR /var/lib/lavinmq
ENV GC_UNMAP_THRESHOLD=1
HEALTHCHECK CMD ["/usr/bin/lavinmqctl", "status"]
ENTRYPOINT ["/usr/bin/lavinmq", "-b", "0.0.0.0", "--guest-only-loopback=false"]
