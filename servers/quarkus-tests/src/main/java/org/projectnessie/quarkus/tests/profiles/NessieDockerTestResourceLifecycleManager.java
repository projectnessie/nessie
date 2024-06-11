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
package org.projectnessie.quarkus.tests.profiles;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Map;
import org.projectnessie.testing.keycloak.CustomKeycloakContainer;
import org.projectnessie.testing.nessie.NessieContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static NessieContainer nessie;

  public static NessieContainer getNessie() {
    return nessie;
  }

  private DevServicesContext context;

  private NessieContainer.NessieConfig.Builder containerConfig;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    this.context = context;
  }

  @Override
  public void init(Map<String, String> initArgs) {
    containerConfig = NessieContainer.builder().fromProperties(initArgs);
    context.containerNetworkId().ifPresent(containerConfig::dockerNetworkId);
  }

  @Override
  public Map<String, String> start() {
    CustomKeycloakContainer keycloak = KeycloakTestResourceLifecycleManager.getKeycloak();
    nessie =
        containerConfig
            .oidcHostIp(keycloak.getExternalIp())
            .oidcInternalRealmUri(keycloak.getInternalRealmUri().toString())
            .oidcTokenIssuerUri(keycloak.getInternalRealmUri().toString())
            .build()
            .createContainer();

    LOGGER.info("Starting Nessie container...");
    nessie.start();

    URI nessieUri = nessie.getExternalNessieUri();
    LOGGER.info("Nessie container started, URI for external clients: {}", nessieUri);

    return Map.of("nessie.iceberg.nessie-client.\"nessie.uri\"", nessieUri.toString());
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
}
