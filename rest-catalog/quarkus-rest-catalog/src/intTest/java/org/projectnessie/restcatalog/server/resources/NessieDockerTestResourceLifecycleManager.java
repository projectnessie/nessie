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
package org.projectnessie.restcatalog.server.resources;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.CustomKeycloakContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Quarkus test resource lifecycle manager for Nessie running on Docker.
 *
 * <p>Uses the {@code ghcr.io/projectnessie/nessie} image by default, but can be configured to use
 * custom images.
 *
 * <p>Note: consider using the Nessie AppRunner plugin for running an in-tree Nessie server for your
 * integration tests. This class is only useful if you need to configure Nessie to interact with
 * other test resources (which the AppRunner plugin can't do), and only if you don't mind running a
 * different Nessie server than the one you're developing.
 */
public class NessieDockerTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {

  /** Annotation to inject the Nessie URI into a test class field. */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NessieDockerUri {}

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NessieDockerTestResourceLifecycleManager.class);

  public static final String NESSIE_DOCKER_IMAGE = "nessie.docker.image";
  public static final String NESSIE_DOCKER_TAG = "nessie.docker.tag";
  public static final String NESSIE_DOCKER_NETWORK_ID = "nessie.docker.network.id";
  public static final String NESSIE_DOCKER_AUTH_ENABLED = "nessie.docker.auth.enabled";
  public static final String NESSIE_DOCKER_DEBUG_ENABLED = "nessie.docker.debug.enabled";

  private static NessieContainer nessie;

  public static NessieContainer getNessie() {
    return nessie;
  }

  private DevServicesContext context;
  private String dockerImage;
  private String dockerTag;
  private String dockerNetworkId;
  private boolean authEnabled;
  private boolean debugEnabled;
  private Map<String, String> extraEnvVars;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  public void init(Map<String, String> initArgs) {
    dockerImage = initArg(initArgs, NESSIE_DOCKER_IMAGE, "ghcr.io/projectnessie/nessie");
    dockerTag = initArg(initArgs, NESSIE_DOCKER_TAG, "latest");
    dockerNetworkId = initArg(initArgs, NESSIE_DOCKER_NETWORK_ID, null);
    authEnabled = initArg(initArgs, NESSIE_DOCKER_AUTH_ENABLED, "false").equalsIgnoreCase("true");
    debugEnabled = initArg(initArgs, NESSIE_DOCKER_DEBUG_ENABLED, "false").equalsIgnoreCase("true");
    extraEnvVars =
        initArgs.entrySet().stream()
            .filter(e -> !e.getKey().startsWith("nessie.docker."))
            .map(e -> Map.entry(asEnvVar(e.getKey()), e.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String initArg(Map<String, String> initArgs, String initArg, String defaultValue) {
    return initArgs.getOrDefault(initArg, System.getProperty(initArg, defaultValue));
  }

  private static String asEnvVar(String key) {
    return key.replaceAll("[.\\-\"]", "_").toUpperCase();
  }

  @Override
  public Map<String, String> start() {

    LOGGER.info("Using Nessie image {}:{}", dockerImage, dockerTag);
    nessie = new NessieContainer();

    LOGGER.info("Starting Nessie container...");
    nessie.start();

    URI nessieUri = nessie.getExternalNessieUri();
    LOGGER.info("Nessie container started, URI for external clients: {}", nessieUri);

    return Map.of(
        "nessie.iceberg.nessie-client.\"" + NessieConfigConstants.CONF_NESSIE_URI + "\"",
        nessieUri.toString());
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        nessie.getExternalNessieUri(),
        new TestInjector.AnnotatedAndMatchesType(NessieDockerUri.class, URI.class));
  }

  @Override
  public void stop() {
    try {
      NessieContainer nessie = NessieDockerTestResourceLifecycleManager.nessie;
      if (nessie != null) {
        nessie.stop();
      }
    } finally {
      NessieDockerTestResourceLifecycleManager.nessie = null;
    }
  }

  public class NessieContainer extends GenericContainer<NessieContainer> {

    @SuppressWarnings("resource")
    public NessieContainer() {
      super(dockerImage + ":" + dockerTag);

      withExposedPorts(19120);
      withNetworkAliases("nessie");
      waitingFor(Wait.forLogMessage(".*Nessie.*started.*Listening on.*", 1));
      withLogConsumer(new Slf4jLogConsumer(logger()));

      // Don't use withNetworkMode, or aliases won't work!
      // See https://github.com/testcontainers/testcontainers-java/issues/1221
      if (context.containerNetworkId().isPresent()) {
        withNetwork(new ExternalNetwork(context.containerNetworkId().get()));
      } else if (dockerNetworkId != null) {
        withNetwork(new ExternalNetwork(dockerNetworkId));
      }

      if (authEnabled) {
        LOGGER.info("Enabling Nessie authentication");
        // If auth is enabled, assume the following:
        // - Keycloak is running on the same Docker network
        // - Nessie will contact Keycloak inside Docker network, using its internal address
        //   with the network alias "keycloak" and the non-mapped HTTP or HTTPS port.
        // - Nessie will also use the configured URI for OIDC token validation,
        //   since Keycloak is configured to return tokens with that specific address as the issuer
        //   claim, regardless of the client IP address.
        CustomKeycloakContainer keycloak = KeycloakTestResourceLifecycleManager.getKeycloak();
        assert keycloak != null;
        withEnv("NESSIE_SERVER_AUTHENTICATION_ENABLED", "true")
            .withEnv("QUARKUS_OIDC_AUTH_SERVER_URL", keycloak.getInternalRealmUri().toString())
            .withEnv("QUARKUS_OIDC_TOKEN_ISSUER", keycloak.getTokenIssuerUri().toString())
            .withEnv("QUARKUS_OIDC_CLIENT_ID", "nessie");
      } else {
        LOGGER.info("Disabling Nessie authentication");
        withEnv("NESSIE_SERVER_AUTHENTICATION_ENABLED", "false");
      }

      if (debugEnabled) {
        withEnv("QUARKUS_LOG_LEVEL", "DEBUG")
            .withEnv("QUARKUS_LOG_CONSOLE_LEVEL", "DEBUG")
            .withEnv("QUARKUS_LOG_MIN_LEVEL", "DEBUG");
      }

      extraEnvVars.forEach(this::withEnv);
    }

    /**
     * Returns the mapped port for the Nessie server, for clients outside the container's network.
     */
    public int getExternalNessiePort() {
      return getMappedPort(19120);
    }

    /** Returns the Nessie URI for external clients running outside the container's network. */
    @SuppressWarnings("HttpUrlsUsage")
    public URI getExternalNessieUri() {
      return URI.create(String.format("http://%s:%d/api/v2", getHost(), getExternalNessiePort()));
    }
  }

  @SuppressWarnings("NullableProblems")
  private static class ExternalNetwork implements Network {

    private final String networkId;

    public ExternalNetwork(String networkId) {
      this.networkId = networkId;
    }

    @Override
    public Statement apply(Statement var1, Description var2) {
      return null;
    }

    public String getId() {
      return networkId;
    }

    public void close() {
      // don't close the external network
    }
  }
}
