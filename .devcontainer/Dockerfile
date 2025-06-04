# Nessie Development Container
# Based on the project's requirements and existing Docker setup

FROM openjdk:21-jdk-slim

LABEL org.opencontainers.image.source=https://github.com/pirate-baby/nessie
LABEL org.opencontainers.image.description="Nessie Development Container"
LABEL org.opencontainers.image.licenses=Apache-2.0

# Set environment variables
ENV LANGUAGE='en_US:en'
ENV JAVA_HOME=/usr/local/openjdk-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install development tools and dependencies
USER root

# Update packages and install development essentials
RUN apt-get update && \
    apt-get install -y \
    git \
    curl \
    wget \
    findutils \
    tar \
    gzip \
    unzip \
    procps \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI (for potential Docker-in-Docker scenarios)
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-24.0.7.tgz | \
    tar -xz -C /tmp && \
    mv /tmp/docker/docker /usr/local/bin/ && \
    rm -rf /tmp/docker

# Create nessie user for development (matching project's pattern)
RUN groupadd --gid 10001 nessie && \
    useradd --uid 10000 --gid nessie --create-home --shell /bin/bash nessie && \
    usermod -aG sudo nessie && \
    echo "nessie ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Set up workspace directory
RUN mkdir -p /workspace && \
    chown -R nessie:nessie /workspace

# Switch to nessie user
USER nessie
WORKDIR /workspace

# Set user environment
ENV USER=nessie
ENV UID=10000
ENV HOME=/home/nessie

# Verify Java installation
RUN java --version && javac --version

# Set the default command
CMD ["/bin/bash"]