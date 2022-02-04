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
package org.projectnessie.server.authz;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;

@ApplicationScoped
public class CelAuthorizer implements Authorizer {
  private final QuarkusNessieAuthorizationConfig config;
  private final CompiledAuthorizationRules compiledRules;

  @Inject
  public CelAuthorizer(
      QuarkusNessieAuthorizationConfig config, CompiledAuthorizationRules compiledRules) {
    this.config = config;
    this.compiledRules = compiledRules;
  }

  @Override
  public BatchAccessChecker startAccessCheck(AccessContext context) {
    if (!config.enabled()) {
      return AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;
    }

    return new CelBatchAccessChecker(compiledRules, context);
  }
}
