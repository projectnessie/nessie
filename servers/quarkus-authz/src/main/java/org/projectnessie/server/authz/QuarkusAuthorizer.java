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
package org.projectnessie.server.authz;

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusNessieAuthorizationConfig;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthorizerType;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Startup
public class QuarkusAuthorizer implements Authorizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuarkusAuthorizer.class);
  private final Authorizer authorizer;

  @Inject
  public QuarkusAuthorizer(
      QuarkusNessieAuthorizationConfig config, @Any Instance<Authorizer> authorizers) {
    if (config.enabled()) {
      if (authorizers.isUnsatisfied()) {
        throw new IllegalStateException("No Nessie Authorizer available");
      }
      Instance<Authorizer> authorizerInstance =
          authorizers.select(new AuthorizerType.Literal(config.authorizationType()));
      if (authorizerInstance.isUnsatisfied()) {
        throw new IllegalStateException(
            "No Nessie Authorizer of type '" + config.authorizationType() + "' available");
      }

      LOGGER.info("Using Nessie {} Authorizer", config.authorizationType());

      this.authorizer = authorizerInstance.get();
    } else {
      this.authorizer = (context, apiContext) -> AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;
    }
  }

  @Override
  public BatchAccessChecker startAccessCheck(AccessContext context, ApiContext apiContext) {
    return this.authorizer.startAccessCheck(context, apiContext);
  }
}
