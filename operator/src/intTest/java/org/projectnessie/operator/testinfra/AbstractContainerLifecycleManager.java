/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.operator.testinfra;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.Objects;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.Network.NetworkImpl;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractContainerLifecycleManager<C extends GenericContainer<?>>
    implements QuarkusTestResourceLifecycleManager {

  protected C container;

  private String inDockerIpAddress;

  protected AbstractContainerLifecycleManager() {}

  @Override
  public Map<String, String> start() {
    inDockerIpAddress = null;
    Logger logger = LoggerFactory.getLogger(getClass());
    container = createContainer();
    container
        .withNetwork(Network.SHARED)
        .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix(container.getDockerImageName()))
        .withStartupAttempts(3);
    container.start();
    return quarkusConfig();
  }

  protected Map<String, String> quarkusConfig() {
    return Map.of();
  }

  protected DockerImageName dockerImage(String name) {
    return ContainerSpecHelper.builder()
        .name(name)
        .containerClass(this.getClass())
        .build()
        .dockerImageName(null);
  }

  protected abstract C createContainer();

  /**
   * The "in-docker" IP address of the container. This IP address is addressable from a deployment
   * running in the K3s container, contrary to the address returned by `container.getHost()` or any
   * of the network aliases defined for the container.
   */
  protected String getInDockerIpAddress() {
    if (inDockerIpAddress == null) {
      inDockerIpAddress = resolveInDockerIpAddress();
      Objects.requireNonNull(
          inDockerIpAddress, "Failed to resolve container's in-docker IP address");
    }
    return inDockerIpAddress;
  }

  private String resolveInDockerIpAddress() {
    return container
        .getCurrentContainerInfo()
        .getNetworkSettings()
        .getNetworks()
        .get(((NetworkImpl) Network.SHARED).getName())
        .getIpAddress();
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }
}
