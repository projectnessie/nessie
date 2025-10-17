/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.testing.nessie;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

public class NessieContainer extends GenericContainer<NessieContainer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieContainer.class);

  public static final String NESSIE_DOCKER_IMAGE = "nessie.docker.image";
  public static final String NESSIE_DOCKER_TAG = "nessie.docker.tag";
  public static final String NESSIE_DOCKER_NETWORK_ID = "nessie.docker.network.id";
  public static final String NESSIE_DOCKER_AUTH_ENABLED = "nessie.docker.auth.enabled";
  public static final String NESSIE_DOCKER_DEBUG_ENABLED = "nessie.docker.debug.enabled";

  public static NessieConfig.Builder builder() {
    return ImmutableNessieConfig.builder();
  }

  @Value.Immutable
  public abstract static class NessieConfig {

    public static final String DEFAULT_IMAGE;
    public static final String DEFAULT_TAG;

    static {
      URL resource = NessieConfig.class.getResource("Dockerfile-nessie-version");
      try (InputStream in = resource.openConnection().getInputStream()) {
        String[] imageTag =
            Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
                .map(String::trim)
                .filter(l -> l.startsWith("FROM "))
                .map(l -> l.substring(5).trim().split(":"))
                .findFirst()
                .orElseThrow();
        DEFAULT_IMAGE = imageTag[0];
        DEFAULT_TAG = imageTag[1];
      } catch (Exception e) {
        throw new RuntimeException("Failed to extract tag from " + resource, e);
      }
    }

    @Value.Default
    public String dockerImage() {
      return DEFAULT_IMAGE;
    }

    @Value.Default
    public String dockerTag() {
      return DEFAULT_TAG;
    }

    @Nullable
    public abstract String dockerNetworkId();

    @Value.Default
    public boolean debugEnabled() {
      return false;
    }

    @Value.Default
    public boolean authEnabled() {
      return false;
    }

    @Nullable
    public abstract String oidcInternalRealmUri();

    @Nullable
    public abstract String oidcTokenIssuerUri();

    @Value.Default
    public String oidcHostName() {
      return "keycloak";
    }

    @Nullable
    public abstract String oidcHostIp();

    public abstract Map<String, String> extraEnvVars();

    public interface Builder {
      @CanIgnoreReturnValue
      Builder dockerImage(String dockerImage);

      @CanIgnoreReturnValue
      Builder dockerTag(String dockerTag);

      @CanIgnoreReturnValue
      Builder dockerNetworkId(String dockerNetworkId);

      @CanIgnoreReturnValue
      Builder debugEnabled(boolean debugEnabled);

      @CanIgnoreReturnValue
      Builder authEnabled(boolean authEnabled);

      @CanIgnoreReturnValue
      Builder oidcInternalRealmUri(String oidcInternalRealmUri);

      @CanIgnoreReturnValue
      Builder oidcTokenIssuerUri(String oidcTokenIssuerUri);

      @CanIgnoreReturnValue
      Builder oidcHostName(String oidcHostName);

      @CanIgnoreReturnValue
      Builder oidcHostIp(String oidcHostIp);

      @CanIgnoreReturnValue
      Builder putExtraEnvVars(String key, String value);

      @CanIgnoreReturnValue
      Builder putExtraEnvVars(Map.Entry<String, ? extends String> envVar);

      @CanIgnoreReturnValue
      Builder extraEnvVars(Map<String, ? extends String> entries);

      NessieConfig build();

      default Builder fromProperties(Map<String, String> initArgs) {
        initArgs(initArgs, NESSIE_DOCKER_DEBUG_ENABLED, s -> debugEnabled(Boolean.parseBoolean(s)));
        initArgs(initArgs, NESSIE_DOCKER_IMAGE, this::dockerImage);
        initArgs(initArgs, NESSIE_DOCKER_TAG, this::dockerTag);
        initArgs(initArgs, NESSIE_DOCKER_NETWORK_ID, this::dockerNetworkId);
        initArgs(initArgs, NESSIE_DOCKER_AUTH_ENABLED, s -> authEnabled(Boolean.parseBoolean(s)));

        initArgs.entrySet().stream()
            .filter(e -> !e.getKey().startsWith("nessie.docker."))
            .map(e -> Map.entry(asEnvVar(e.getKey()), e.getValue()))
            .forEach(this::putExtraEnvVars);

        return this;
      }
    }

    public NessieContainer createContainer() {
      LOGGER.info("Using Nessie image {}:{}", dockerImage(), dockerTag());
      return new NessieContainer(this);
    }
  }

  private static String asEnvVar(String key) {
    return key.replaceAll("[.\\-\"]", "_").toUpperCase();
  }

  private static void initArgs(
      Map<String, String> initArgs, String property, Consumer<String> consumer) {
    String value = initArgs.getOrDefault(property, System.getProperty(property));
    if (value != null) {
      consumer.accept(value);
    }
  }

  @SuppressWarnings("resource")
  public NessieContainer(NessieConfig config) {
    super(config.dockerImage() + ":" + config.dockerTag());

    withExposedPorts(19120, 9000);
    withNetworkAliases("nessie");
    Duration startupTimeout = Duration.ofMinutes(2);
    waitingFor(
        new WaitAllStrategy()
            .withStrategy(Wait.forHttp("/q/health/live").forPort(9000))
            .withStrategy(Wait.forListeningPorts(19120))
            .withStartupTimeout(startupTimeout));
    withStartupTimeout(startupTimeout);
    withLogConsumer(new Slf4jLogConsumer(logger()));

    // Don't use withNetworkMode, or aliases won't work!
    // See https://github.com/testcontainers/testcontainers-java/issues/1221
    String containerNetworkId = config.dockerNetworkId();
    if (containerNetworkId != null) {
      withNetwork(new ExternalNetwork(containerNetworkId));
    }

    if (config.authEnabled()) {
      LOGGER.info("Enabling Nessie authentication");
      // If auth is enabled, assume the following:
      // - Keycloak is running on the same Docker network
      // - Nessie will contact Keycloak inside Docker network, using its internal address
      //   with the network alias "keycloak" and the non-mapped HTTP or HTTPS port.
      // - Nessie will also use the configured URI for OIDC token validation,
      //   since Keycloak is configured to return tokens with that specific address as the issuer
      //   claim, regardless of the client IP address.

      if (config.oidcHostIp() != null) {
        withExtraHost(config.oidcHostName(), config.oidcHostIp());
      }

      withEnv("NESSIE_SERVER_AUTHENTICATION_ENABLED", "true");
      if (config.oidcInternalRealmUri() != null) {
        withEnv("QUARKUS_OIDC_AUTH_SERVER_URL", config.oidcInternalRealmUri());
      }
      if (config.oidcInternalRealmUri() != null) {
        withEnv("QUARKUS_OIDC_TOKEN_ISSUER", config.oidcTokenIssuerUri());
      }
      withEnv("QUARKUS_OIDC_CLIENT_ID", "nessie");
    } else {
      LOGGER.info("Disabling Nessie authentication");
      withEnv("NESSIE_SERVER_AUTHENTICATION_ENABLED", "false");
    }

    if (config.debugEnabled()) {
      withEnv("QUARKUS_LOG_LEVEL", "DEBUG")
          .withEnv("QUARKUS_LOG_CONSOLE_LEVEL", "DEBUG")
          .withEnv("QUARKUS_LOG_MIN_LEVEL", "DEBUG");
    }

    config.extraEnvVars().forEach(this::withEnv);
  }

  /** Returns the mapped port for the Nessie server, for clients outside the container's network. */
  public int getExternalNessiePort() {
    return getMappedPort(19120);
  }

  /** Returns the Nessie URI for external clients running outside the container's network. */
  public URI getExternalNessieUri() {
    return getExternalNessieBaseUri().resolve("v2");
  }

  public URI getExternalNessieBaseUri() {
    return URI.create(String.format("http://%s:%d/api/", getHost(), getExternalNessiePort()));
  }

  private static class ExternalNetwork implements Network {

    private final String networkId;

    public ExternalNetwork(String networkId) {
      this.networkId = networkId;
    }

    public String getId() {
      return networkId;
    }

    public void close() {
      // don't close the external network
    }
  }
}
