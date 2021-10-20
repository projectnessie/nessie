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
package org.projectnessie.server.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.Map;

/** Configuration for Nessie authorization settings. */
@ConfigMapping(prefix = "nessie.server.authorization")
public interface QuarkusNessieAuthorizationConfig {

  /**
   * Returns {@code true} if Nessie authorization is enabled.
   *
   * @return {@code true} if Nessie authorization is enabled.
   */
  @WithName("enabled")
  @WithDefault("false")
  boolean enabled();

  /**
   * The authorization rules where the key represents the rule id and the value the CEL expression.
   *
   * @return The authorization rules where the key represents the rule id and the value the CEL
   *     expression.
   */
  Map<String, String> rules();
}
