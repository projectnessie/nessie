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
package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;

@QuarkusTest
@TestProfile(TestMetricsTags.Profile.class)
public class TestMetricsTags extends AbstractQuarkusRestWithMetrics {

  @Test
  void smokeTestMetricsWithTags() {
    String[] lines = getMetrics().split("\n");
    assertThat(lines)
        .anySatisfy(
            line ->
                assertThat(line)
                    .contains("nessie_versionstore_request_seconds_max")
                    .contains("application=\"NessieDev\"")
                    .contains("service=\"nessie\"")
                    .contains("environment=\"dev\""));
  }

  @Override
  protected String getApplicationLabel() {
    return "NessieDev";
  }

  public static class Profile extends QuarkusTestProfilePersistInmemory {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>(super.getConfigOverrides());
      config.put("nessie.metrics.tags.application", "NessieDev");
      config.put("nessie.metrics.tags.service", "nessie");
      config.put("nessie.metrics.tags.environment", "dev");
      return config;
    }
  }
}
