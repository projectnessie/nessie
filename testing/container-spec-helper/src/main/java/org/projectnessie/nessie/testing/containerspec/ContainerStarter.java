/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.nessie.testing.containerspec;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;

/**
 * Starts Testcontainers containers with retries and verifies that a container is still running
 * after {@link GenericContainer#start()} returned.
 *
 * <p>Testcontainers' {@code HostPortWaitStrategy} ignores a failed (not timed out) in-container
 * port check, so {@link GenericContainer#start()} can return successfully although the container
 * process already died. This class treats such a container as a failed launch, logs the container
 * state and logs, and retries.
 */
public final class ContainerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerStarter.class);

  public static final int DEFAULT_MAX_RETRIES = 3;
  private static final int MAX_LOG_LINES = 100;

  private ContainerStarter() {}

  /**
   * Creates and starts a container using {@code containerFactory}, retrying up to {@value
   * #DEFAULT_MAX_RETRIES} times.
   */
  public static <C extends GenericContainer<?>> C startContainer(Supplier<C> containerFactory) {
    return startContainer(containerFactory, DEFAULT_MAX_RETRIES);
  }

  /**
   * Creates and starts a container using {@code containerFactory}, retrying up to {@code
   * maxRetries} times, if the launch failed with a cause or the container is not running after
   * {@link GenericContainer#start()} returned.
   *
   * @return the started container, which is running
   * @throws RuntimeException if the container could not be started
   */
  public static <C extends GenericContainer<?>> C startContainer(
      Supplier<C> containerFactory, int maxRetries) {
    for (int retry = 0; ; retry++) {
      C c = containerFactory.get();
      try {
        c.start();
        verifyRunning(c);
        return c;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < maxRetries) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        c.close();
        throw e;
      }
    }
  }

  /**
   * Throws a {@link ContainerLaunchException} with diagnostic information, if the given container
   * is not running.
   */
  public static void verifyRunning(GenericContainer<?> container) {
    if (container.isRunning()) {
      return;
    }

    String state = describeState(container);
    String logs = tailLogs(container);
    LOGGER.error(
        "Container {} is not running after start: {}\nLast container log lines:\n{}",
        container.getDockerImageName(),
        state,
        logs);
    throw new ContainerLaunchException(
        "Container " + container.getDockerImageName() + " is not running after start",
        new IllegalStateException(
            "Container " + container.getDockerImageName() + " is not running: " + state));
  }

  private static String describeState(GenericContainer<?> container) {
    try {
      InspectContainerResponse.ContainerState state =
          container.getCurrentContainerInfo().getState();
      return String.format(
          "status=%s, exitCode=%s, oomKilled=%s, error=%s, startedAt=%s, finishedAt=%s",
          state.getStatus(),
          state.getExitCodeLong(),
          state.getOOMKilled(),
          state.getError(),
          state.getStartedAt(),
          state.getFinishedAt());
    } catch (Exception e) {
      return "<failed to inspect container: " + e + ">";
    }
  }

  private static String tailLogs(GenericContainer<?> container) {
    try {
      List<String> lines = Arrays.asList(container.getLogs().split("\n"));
      return String.join(
          "\n", lines.subList(Math.max(0, lines.size() - MAX_LOG_LINES), lines.size()));
    } catch (Exception e) {
      return "<failed to retrieve container logs: " + e + ">";
    }
  }
}
