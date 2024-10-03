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
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;
import org.projectnessie.server.events.EventsEnabledProfile;

@QuarkusTest
@TestProfile(TestQuarkusEventsEnabledAuthEnabled.Profile.class)
@NessieApiVersions(versions = NessieApiVersion.V2)
public class TestQuarkusEventsEnabledAuthEnabled extends AbstractQuarkusEvents {

  public static class Profile extends EventsEnabledProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("nessie.version.store.type", IN_MEMORY.name())
          .put("quarkus.http.auth.basic", "true")
          // Need a dummy URL to satisfy the Quarkus OIDC extension.
          .put("quarkus.oidc.auth-server-url", "http://127.255.0.0:0/auth/realms/unset/")
          .putAll(AuthenticationEnabledProfile.AUTH_CONFIG_OVERRIDES)
          .putAll(AuthenticationEnabledProfile.SECURITY_CONFIG)
          .build();
    }
  }

  @Inject Instance<MeterRegistry> registries;

  @Override
  protected NessieClientBuilder customizeClient(
      NessieClientBuilder builder, NessieApiVersion apiVersion) {
    return builder.withAuthentication(BasicAuthenticationProvider.create("test_user", "test_user"));
  }

  @Override
  protected boolean eventsEnabled() {
    return true;
  }

  @Override
  protected String eventInitiator() {
    return "test_user";
  }

  @Override
  protected void clearMetrics() {
    registries.stream().forEach(MeterRegistry::clear);
  }
}
