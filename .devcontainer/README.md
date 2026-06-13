# Nessie Development Container

This directory contains a development container configuration for Project Nessie that provides a consistent, containerized development environment with all necessary tools pre-installed.

## What's Included

- **Java 21 JDK** - Full OpenJDK development environment
- **Gradle** - Build tool (downloaded automatically via wrapper)
- **Git** - Version control
- **Docker CLI** - For container operations
- **Development tools** - curl, wget, unzip, and other essentials
- **VS Code extensions** - Java development pack, Gradle support
- **User setup** - Non-root `nessie` user with sudo access

## Quick Start

### Option 1: VS Code Dev Containers (Recommended)

1. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open the project in VS Code
3. When prompted, click "Reopen in Container" or use `Ctrl+Shift+P` → "Dev Containers: Reopen in Container"
4. Wait for the container to build and start

### Option 2: Manual Docker Usage

```bash
# Build the development container
docker build -f .devcontainer/Dockerfile -t nessie-dev .

# Run a shell in the container
docker run -it --rm -v $(pwd):/workspace -w /workspace nessie-dev /bin/bash

# Test the setup
./gradlew --version
java --version
```

## Development Workflow

Once in the container, you can use all the standard Nessie [development commands](../CONTRIBUTING.md):

The Nessie server will be available at http://localhost:19120 (automatically forwarded in VS Code).

## Why Use This Dev Container?

This addresses the common issues mentioned in Nessie's contributing docs:

- ✅ **No "development on the metal"** - Everything runs in a safe, isolated container
- ✅ **Linux-first environment** - Even on macOS/Windows, you get a consistent Linux environment
- ✅ **No Podman conflicts** - Uses Docker with a standard Debian base
- ✅ **Pre-configured toolchain** - Java 21, Gradle, and all dependencies ready to go
- ✅ **VS Code integration** - Full IDE support with proper Java language server setup


## Troubleshooting

**Container build fails with "At least one invalid signature was encountered."**: You are likely out of disk space (in docker at least), or you have no network connection. Clean out unused containers and images, and try again.