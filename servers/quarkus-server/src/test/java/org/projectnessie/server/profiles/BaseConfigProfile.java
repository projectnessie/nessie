/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.server.profiles;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class BaseConfigProfile implements QuarkusTestProfile {

  public static final Map<String, String> CONFIG_OVERRIDES =
      ImmutableMap.<String, String>builder().put("quarkus.jaeger.sampler-type", "const").build();

  public static final Map<String, String> VERSION_STORE_CONFIG =
      ImmutableMap.<String, String>builder()
          .put("nessie.version.store.advanced.key-prefix", "nessie-test")
          .put("nessie.version.store.advanced.commit-retries", "42")
          .put("nessie.version.store.advanced.tx.batch-size", "41")
          .build();

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(CONFIG_OVERRIDES)
        .putAll(VERSION_STORE_CONFIG)
        .build();
  }
}
