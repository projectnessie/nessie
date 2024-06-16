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

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.JDBC;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class QuarkusTestProfilePersistH2 extends BaseConfigProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(super.getConfigOverrides())
        .put("nessie.version.store.type", JDBC.name())
        .put("nessie.version.store.persist.jdbc.datasource", "h2")
        .build();
  }
}
