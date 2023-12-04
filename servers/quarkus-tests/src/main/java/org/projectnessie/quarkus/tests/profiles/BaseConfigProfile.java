/*
 * Copyright (C) 2022 Dremio
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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class BaseConfigProfile implements QuarkusTestProfile {

  public static final String TEST_REPO_ID = "nessie-test";

  public static final Map<String, String> CONFIG_OVERRIDES =
      ImmutableMap.<String, String>builder().build();

  public static final Map<String, String> VERSION_STORE_CONFIG;

  static {
    ImmutableMap.Builder<String, String> config =
        ImmutableMap.<String, String>builder()
            .put("nessie.version.store.persist.repository-id", TEST_REPO_ID)
            .put("nessie.version.store.persist.commit-retries", "42");

    // Pass relevant system properties to Quarkus integration tests, which are run "externally"
    // in a Docker container. This code is rather a "no op" for Quarkus unit tests.
    System.getProperties().entrySet().stream()
        .filter(
            e -> {
              String key = e.getKey().toString();
              return key.startsWith("user.") || key.startsWith("file.") || key.startsWith("path.");
            })
        .forEach(e -> config.put(e.getKey().toString(), e.getValue().toString()));

    VERSION_STORE_CONFIG = config.build();
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(CONFIG_OVERRIDES)
        .putAll(VERSION_STORE_CONFIG)
        .build();
  }
}
