FROM 84codes/crystal:latest-ubuntu-24.04

# Avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    openssl \
    sudo \
    zsh \
    kmod \
    liblz4-dev \
    dpkg-dev \
    git \
    etcd-server \
    tcpdump \
    && rm -rf /var/lib/apt/lists/*

# Set environment for kTLS
ENV SSL_KTLS=1

WORKDIR /workspace
