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
package org.projectnessie.client.ext;

import java.util.HashMap;
import java.util.Map;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;

/**
 * Test classes that implement this interface can specify a different Nessie client by {@linkplain
 * NessieClientBuilder#name() its name} and optionally a set of configuration options.
 */
@FunctionalInterface
public interface NessieClientNameResolver {
  String nessieClientName();

  /**
   * When overriding this function, make sure to use the map of this implementation to include the
   * {@link #nessieClientName()}.
   */
  default Map<String, String> mainNessieClientConfigMap() {
    Map<String, String> mainConfig = new HashMap<>();
    mainConfig.put(NessieConfigConstants.CONF_NESSIE_CLIENT_NAME, nessieClientName());
    return mainConfig;
  }
}
