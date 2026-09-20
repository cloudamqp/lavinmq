# Build with:
#   docker buildx build -f packaging/static-build/Dockerfile --output=bin .

FROM 84codes/crystal:latest-alpine AS builder
RUN apk add curl openssl lz4-dev lz4-static
WORKDIR /tmp
COPY shard.yml shard.lock .
RUN shards install --production
COPY ./static ./static
COPY ./views ./views
COPY ./src ./src
COPY Makefile .
ARG MAKEFLAGS=-j2
RUN make all 'CRYSTAL_FLAGS=--release --static'

FROM scratch
COPY --from=builder /tmp/bin/* /
