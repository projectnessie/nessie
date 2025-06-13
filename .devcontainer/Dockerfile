FROM openjdk:21-jdk-slim

LABEL org.opencontainers.image.source=https://github.com/projectnessie/nessie
LABEL org.opencontainers.image.description="Nessie Development Container"
LABEL org.opencontainers.image.licenses=Apache-2.0

ENV LANGUAGE='en_US:en'
ENV JAVA_HOME=/usr/local/openjdk-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# note "invalid signature encountered" is a common error when out of disk space or no internet
RUN apt-get update && \
    apt-get install -y \
    git \
    curl \
    wget \
    findutils \
    tar \
    gzip \
    unzip \
    procps && \
    rm -rf /var/lib/apt/lists/*

# will need docker/podman
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

USER nessie
WORKDIR /workspace

# verify
RUN java --version && javac --version
CMD ["/bin/bash"]