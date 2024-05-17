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
package org.projectnessie.tools.admin.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.Test;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;

@QuarkusMainTest
@TestProfile(QuarkusTestProfilePersistInmemory.class)
class TestNessieServerAdminTool {

  @Test
  @Launch("--help")
  public void testHelp(LaunchResult result) {
    assertThat(result.getOutput()).contains("Show this help message and exit");
  }

  @Test
  @Launch(value = {})
  public void testNoArgs(LaunchResult result) {
    assertThat(result.getOutput()).contains("Use the 'help' command");

    assertThat(result.getErrorOutput())
        .contains(
            "Repository information & maintenance for an in-memory implementation is meaningless");
  }
}
