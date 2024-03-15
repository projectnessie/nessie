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
package org.projectnessie.server.events;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;

/** A simple {@link QuarkusTestProfile} that enables Nessie events. */
public class EventsEnabledProfile extends BaseConfigProfile {

  public static final Map<String, String> EVENTS_CONFIG =
      ImmutableMap.<String, String>builder()
          .put("nessie.version.store.events.enable", "true")
          .put("quarkus.micrometer.enabled", "true")
          .put("quarkus.otel.traces.sampler", "always_on")
          .put("nessie.version.store.events.retry.initial-delay", "PT0.1S")
          .put("nessie.version.store.events.static-properties.foo", "bar")
          .build();

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.<String, String>builder()
        .putAll(super.getConfigOverrides())
        .putAll(EVENTS_CONFIG)
        .build();
  }
}
