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
package org.projectnessie.server.authz;

import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;

/**
 * Compiles the authorization rules from {@link QuarkusNessieAuthorizationConfig} at startup and
 * provides access to them via {@link QuarkusCompiledAuthorizationRules#getRules()}.
 */
@Singleton
@Startup
public class QuarkusCompiledAuthorizationRules extends CompiledAuthorizationRules {
  @Inject
  public QuarkusCompiledAuthorizationRules(QuarkusNessieAuthorizationConfig config) {
    super(config.rules());
  }
}
