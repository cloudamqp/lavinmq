
FROM crystallang/crystal:0.33.0-alpine

RUN mkdir /avalanchemq
WORKDIR /avalanchemq

# Installing dependencies
COPY shard.yml /avalanchemq/shard.yml
COPY shard.lock /avalanchemq/shard.lock
RUN shards install

# Copying the code
COPY . /avalanchemq

# Exposing ports are required
EXPOSE 15672 5672

# Start the main process.
CMD ["crystal", "run", "src/avalanchemq.cr", "--" ,"-D", "/data"]