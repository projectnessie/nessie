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
package org.projectnessie.server.catalog.auth;

import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.INIT_ADDRESS;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.S3_INIT_ADDRESS;
import static org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.S3_WAREHOUSE_LOCATION;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticatorConfig;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientId;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientSecret;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakTokenEndpointUri;
import org.projectnessie.server.catalog.AbstractIcebergCatalogTests;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    parallel = true,
    value = KeycloakTestResourceLifecycleManager.class)
@TestProfile(AbstractAuthEnabledTests.Profile.class)
public abstract class AbstractAuthEnabledTests extends AbstractIcebergCatalogTests {

  @KeycloakTokenEndpointUri protected URI tokenEndpoint;

  @KeycloakClientId protected String clientId;

  @KeycloakClientSecret protected String clientSecret;

  HeapStorageBucket heapStorageBucket;

  @BeforeEach
  public void clearBucket() {
    heapStorageBucket.clear();
  }

  @BeforeEach
  public void clearS3SignerClientCache() throws Exception {
    Field authSessionCache = S3V4RestSignerClient.class.getDeclaredField("authSessionCache");
    authSessionCache.setAccessible(true);
    Object cache = authSessionCache.get(null);
    if (cache != null) {
      Method invalidateAll = cache.getClass().getMethod("invalidateAll");
      invalidateAll.setAccessible(true);
      invalidateAll.invoke(cache);
    }
  }

  @AfterEach
  public void closeS3SignerExecutor() throws Exception {
    Field tokenRefreshExecutor =
        S3V4RestSignerClient.class.getDeclaredField("tokenRefreshExecutor");
    tokenRefreshExecutor.setAccessible(true);
    ScheduledExecutorService executor = (ScheduledExecutorService) tokenRefreshExecutor.get(null);
    if (executor != null) {
      List<Runnable> tasks = executor.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });
      tokenRefreshExecutor.set(null, null);
    }
  }

  @Override
  protected NessieClientBuilder nessieClientBuilder() {
    return super.nessieClientBuilder()
        .withAuthentication(
            OAuth2AuthenticationProvider.create(
                OAuth2AuthenticatorConfig.builder()
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tokenEndpoint(tokenEndpoint)
                    .build()));
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "nessie.server.authentication.enabled",
          "true",
          "nessie.catalog.default-warehouse",
          "warehouse",
          "nessie.catalog.warehouses.warehouse.location",
          S3_WAREHOUSE_LOCATION);
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return Collections.singletonList(
          new TestResourceEntry(
              ObjectStorageMockTestResourceLifecycleManager.class,
              ImmutableMap.of(INIT_ADDRESS, S3_INIT_ADDRESS),
              true));
    }
  }

  @Override
  protected String temporaryLocation() {
    return S3_WAREHOUSE_LOCATION + "/temp/" + UUID.randomUUID();
  }
}
