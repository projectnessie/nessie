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

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthorizerType;
import org.projectnessie.services.authz.BatchAccessChecker;

@AuthorizerType("CEL")
@Dependent
public class CelAuthorizer implements Authorizer {
  private final CompiledAuthorizationRules compiledRules;

  @Inject
  public CelAuthorizer(CompiledAuthorizationRules compiledRules) {
    this.compiledRules = compiledRules;
  }

  @Override
  public BatchAccessChecker startAccessCheck(AccessContext context, ApiContext apiContext) {
    return new CelBatchAccessChecker(compiledRules, context, apiContext);
  }
}
