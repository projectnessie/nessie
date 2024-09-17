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
package org.projectnessie.jaxrs.ext;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;
import java.util.function.Function;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;

public class AuthorizerExtension implements Extension {
  private volatile Function<AccessContext, BatchAccessChecker> accessCheckerSupplier;

  private final Authorizer authorizer =
      new Authorizer() {
        @Override
        public BatchAccessChecker startAccessCheck(AccessContext context, ApiContext apiContext) {
          if (accessCheckerSupplier == null) {
            return AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;
          }
          return accessCheckerSupplier.apply(context);
        }
      };

  public AuthorizerExtension setAccessCheckerSupplier(
      Function<AccessContext, BatchAccessChecker> accessCheckerSupplier) {
    this.accessCheckerSupplier = accessCheckerSupplier;
    return this;
  }

  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
    abd.addBean()
        .addType(Authorizer.class)
        .addQualifier(Default.Literal.INSTANCE)
        .scope(ApplicationScoped.class)
        .produceWith(i -> authorizer);
  }
}
