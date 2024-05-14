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
package org.projectnessie.tools.compatibility.jersey;

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.util.TypeLiteral;
import java.security.Principal;
import org.projectnessie.services.authz.AccessContext;

/**
 * A CDI extension that always produces {@code null} {@link Principal} objects simulating execution
 * without authentication.
 */
public class PrincipalExtension implements Extension {
  @SuppressWarnings("unused")
  public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
    AccessContext accessContext = () -> () -> null;

    abd.addBean()
        .addType(new TypeLiteral<AccessContext>() {})
        .addQualifier(Default.Literal.INSTANCE)
        .scope(RequestScoped.class)
        .produceWith(i -> accessContext);
  }
}
