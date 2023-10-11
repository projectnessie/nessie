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
package org.projectnessie.tools.compatibility.tests;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

@ExtendWith(OlderNessieServersExtension.class)
@Tag("nessie-multi-env")
public class ITOlderServers extends AbstractCompatibilityTests {

  @Override
  protected Version serverVersion() {
    return version;
  }

  @Test
  @Override
  void getConfigV1() {
    NessieConfiguration config = api.getConfig();
    assertThat(config.getDefaultBranch()).isEqualTo("main");
    assertThat(config.getMinSupportedApiVersion()).isEqualTo(1);
    assertThat(config.getMaxSupportedApiVersion()).isEqualTo(2);
    assertThat(config.getActualApiVersion()).isEqualTo(0);
    assertThat(config.getSpecVersion()).isNull();
  }

  @Test
  @VersionCondition(minVersion = "0.59.0")
  @Override
  void getConfigV2() {
    NessieConfiguration config = apiV2.getConfig();
    assertThat(config.getDefaultBranch()).isEqualTo("main");
    assertThat(config.getMinSupportedApiVersion()).isEqualTo(1);
    assertThat(config.getMaxSupportedApiVersion()).isEqualTo(2);
    assertThat(config.getActualApiVersion()).isEqualTo(2);
    assertThat(config.getSpecVersion()).isEqualTo("2.0.0");
    if (version.isLessThan(Version.ACTUAL_VERSION_IN_CONFIG_V2)) {
      assertThat(config.getActualApiVersion()).isEqualTo(0);
    } else {
      assertThat(config.getActualApiVersion()).isEqualTo(2);
    }
    if (version.isLessThan(Version.SPEC_VERSION_IN_CONFIG_V2)) {
      assertThat(config.getSpecVersion()).isNull();
    } else if (version.isGreaterThanOrEqual(Version.SPEC_VERSION_IN_CONFIG_V2_GA)) {
      assertThat(config.getSpecVersion()).isEqualTo("2.0.0");
    } else if (version.isLessThan(Version.SPEC_VERSION_IN_CONFIG_V2_SEMVER)) {
      assertThat(config.getSpecVersion()).isEqualTo("2.0-beta.1");
    } else {
      assertThat(config.getSpecVersion()).isEqualTo("2.0.0-beta.1");
    }
  }
}
