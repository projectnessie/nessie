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
package org.projectnessie.server.authz;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import org.projectnessie.server.config.QuarkusServerConfig;

/**
 * A simple {@link QuarkusTestProfile} that enables the Nessie authorization flag, which is fetched
 * by {@link QuarkusServerConfig#authorizationEnabled()}.
 */
public class NessieAuthorizationTestProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return ImmutableMap.of(
        "nessie.server.authorization-enabled",
        "true",
        "nessie.server.authorization.enabled",
        "true");
  }
}
