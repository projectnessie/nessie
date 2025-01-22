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
package org.projectnessie.server.catalog;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.URI;
import java.util.Map;
import org.projectnessie.testing.azurite.AzuriteContainer;

public class AzuriteTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  private final AzuriteContainer azurite = new AzuriteContainer().withStartupAttempts(5);

  private URI warehouseLocation;

  @Override
  public Map<String, String> start() {
    azurite.start();
    warehouseLocation =
        URI.create(
            "abfs://"
                + azurite.storageContainer()
                + "@"
                + azurite.accountFq()
                + "/"
                + azurite.storageContainer());
    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.service.adls.default-options.retry-policy", "EXPONENTIAL_BACKOFF")
        .put("nessie.catalog.service.adls.default-options.auth-type", "STORAGE_SHARED_KEY")
        .put(
            "nessie.catalog.service.adls.default-options.account",
            "urn:nessie-secret:quarkus:my-azurite-account")
        .put("my-azurite-account.name", azurite.account())
        .put("my-azurite-account.secret", azurite.secretBase64())
        .put("nessie.catalog.service.adls.default-options.endpoint", azurite.endpoint())
        .put("nessie.catalog.default-warehouse", "warehouse")
        .put("nessie.catalog.warehouses.warehouse.location", warehouseLocation.toString())
        .build();
  }

  @Override
  public void stop() {
    azurite.stop();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        warehouseLocation,
        new TestInjector.AnnotatedAndMatchesType(WarehouseLocation.class, URI.class));
    testInjector.injectIntoFields(
        azurite.account(),
        new TestInjector.AnnotatedAndMatchesType(WarehouseAccount.class, String.class));
    testInjector.injectIntoFields(
        azurite.secretBase64(),
        new TestInjector.AnnotatedAndMatchesType(WarehouseAccountSecret.class, String.class));
  }
}
