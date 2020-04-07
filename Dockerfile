
FROM crystallang/crystal:0.34.0

WORKDIR /avalanchemq

# Copying the code
COPY . /avalanchemq

# Build
RUN shards build --production --release avalanchemq

# Exposing ports are required
EXPOSE 15672 5672

# Start the main process.
CMD ["./bin/avalanchemq","-D", "/data"]