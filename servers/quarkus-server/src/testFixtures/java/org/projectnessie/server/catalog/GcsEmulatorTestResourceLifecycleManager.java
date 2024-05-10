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
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Map;
import org.projectnessie.testing.gcs.GcsContainer;

public class GcsEmulatorTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager {

  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  public @interface WarehouseLocation {}

  private final GcsContainer gcs = new GcsContainer().withStartupAttempts(5);

  private URI warehouseLocation;

  @Override
  public Map<String, String> start() {
    gcs.start();
    warehouseLocation = URI.create("gs://" + gcs.bucket() + "/warehouse");
    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.service.gcs.host", gcs.baseUri())
        .put("nessie.catalog.service.gcs.project-id", gcs.projectId())
        .put("nessie.catalog.service.gcs.auth-type", "ACCESS_TOKEN")
        .put("nessie.catalog.service.gcs.oauth2-token-ref", "gcsTokenRef")
        .put("nessie.catalog.secrets.gcsTokenRef", gcs.oauth2token())
        .put("nessie.catalog.default-warehouse.name", "warehouse")
        .put("nessie.catalog.default-warehouse.location", warehouseLocation.toString())
        .build();
  }

  @Override
  public void stop() {
    gcs.stop();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        warehouseLocation,
        new TestInjector.AnnotatedAndMatchesType(WarehouseLocation.class, URI.class));
  }
}
