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
package org.projectnessie.server;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.IN_MEMORY;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.server.events.EventsEnabledProfile;

@QuarkusTest
@TestProfile(TestQuarkusEventsEnabledAuthDisabled.Profile.class)
@NessieApiVersions(versions = NessieApiVersion.V2)
public class TestQuarkusEventsEnabledAuthDisabled extends AbstractQuarkusEvents {

  public static class Profile extends EventsEnabledProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("nessie.version.store.type", IN_MEMORY.name())
          .build();
    }
  }

  @Inject Instance<MeterRegistry> registries;

  @Override
  protected boolean eventsEnabled() {
    return true;
  }

  @Override
  protected void clearMetrics() {
    registries.stream().forEach(MeterRegistry::clear);
  }
}
