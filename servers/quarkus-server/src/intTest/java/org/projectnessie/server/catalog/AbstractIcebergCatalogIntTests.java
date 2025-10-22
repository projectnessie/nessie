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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.WithTestResource;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import org.junit.jupiter.api.Test;

@WithTestResource(IcebergResourceLifecycleManager.ForIntegrationTests.class)
public abstract class AbstractIcebergCatalogIntTests extends AbstractIcebergCatalogTests {

  @Test
  public void objectStoreReadiness() throws Exception {
    int managementPort = Integer.getInteger("quarkus.management.port", 9000);
    URL health = URI.create(String.format("http://127.0.0.1:%d/q/health", managementPort)).toURL();
    URLConnection conn = health.openConnection();
    try (var input = conn.getInputStream()) {
      JsonNode healthDoc = new ObjectMapper().readTree(input);
      JsonNode checks = healthDoc.get("checks");
      for (JsonNode check : checks) {
        if (ObjectStoresHealthCheck.NAME.equals(check.get("name").asText())) {
          assertThat(check.get("status").asText()).isEqualTo("UP");
          break;
        }
      }
    }
  }
}
