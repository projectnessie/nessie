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
import io.smallrye.config.WithName;
import java.util.Map;
import org.projectnessie.services.config.AuthorizationRulesConfig;

@ConfigMapping(prefix = "nessie.server.authorization")
public interface QuarkusAuthorizationRulesConfig extends AuthorizationRulesConfig {

  @Override
  @WithName("enabled")
  boolean enabled();

  @Override
  @WithName("rules")
  Map<String, String> rules();
}
