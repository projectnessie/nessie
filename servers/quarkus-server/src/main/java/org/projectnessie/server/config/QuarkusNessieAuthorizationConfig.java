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

import io.quarkus.arc.config.ConfigProperties;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Configuration for Nessie authorization settings. */
@ConfigProperties(prefix = "nessie.server.authorization")
public interface QuarkusNessieAuthorizationConfig {

  /**
   * Returns {@code true} if Nessie authorization is enabled.
   *
   * @return {@code true} if Nessie authorization is enabled.
   */
  @ConfigProperty(name = "enabled", defaultValue = "false")
  boolean enabled();

  /*
   * The authorization rules where the key represents the rule id and the value the CEL expression.
   *
   * @return The authorization rules where the key represents the rule id and the value the CEL
   *     expression.
   */
  // TODO Quarkus/microprofile config API do not allow maps. Using a workaround here until
  //  https://github.com/quarkusio/quarkus/issues/19990 is fixed.
  // Map<String, String> rules();
}
