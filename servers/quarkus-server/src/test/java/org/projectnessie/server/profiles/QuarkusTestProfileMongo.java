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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.projectnessie.server.config.VersionStoreConfig.VersionStoreType;

public class QuarkusTestProfileMongo extends BaseConfigProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(super.getConfigOverrides())
        .put("nessie.version.store.type", VersionStoreType.MONGO.name())
        .put("quarkus.mongodb.write-concern.journal", "false")
        .build();
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return ImmutableList.of(new TestResourceEntry(MongoTestResourceLifecycleManager.class));
  }
}
