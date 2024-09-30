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

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.Annotated;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.MatchesType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.List;
import org.projectnessie.testing.keycloak.CustomKeycloakContainer;
import org.projectnessie.testing.keycloak.ImmutableKeycloakConfig;
import org.testcontainers.containers.wait.strategy.Wait;

public class KeycloakContainerLifecycleManager
    extends AbstractContainerLifecycleManager<CustomKeycloakContainer> {

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface InternalRealmUri {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ExternalRealmUri {}

  public static final String CLIENT_ID = "nessie";

  @Override
  @SuppressWarnings("resource")
  protected CustomKeycloakContainer createContainer() {
    return new CustomKeycloakContainer(
            ImmutableKeycloakConfig.builder()
                .realmConfigure(
                    realm ->
                        realm
                            .getClients()
                            .add(
                                CustomKeycloakContainer.createServiceClient(
                                    CLIENT_ID, List.of("email", "profile"))))
                .build())
        .waitingFor(Wait.forLogMessage(".*Running the server in development mode.*", 1));
  }

  @Override
  public void inject(TestInjector testInjector) {
    // The "keycloak" hostname is not resolvable from within the K3s container, so we need to use
    // the in-Docker IP address of the Keycloak container instead.
    URI internalRealmUri =
        URI.create(
            container.getInternalRealmUri().toString().replace("keycloak", getInDockerIpAddress()));
    URI externalRealmUri = container.getExternalRealmUri();
    testInjector.injectIntoFields(
        internalRealmUri, new MatchesType(URI.class).and(new Annotated(InternalRealmUri.class)));
    testInjector.injectIntoFields(
        externalRealmUri, new MatchesType(URI.class).and(new Annotated(ExternalRealmUri.class)));
  }
}
