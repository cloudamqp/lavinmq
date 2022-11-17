FROM 84codes/crystal:latest-debian-11
WORKDIR /src
COPY shard.* /src
RUN shards install
ENTRYPOINT bash
