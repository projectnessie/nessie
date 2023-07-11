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
package org.projectnessie.server.authn;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.security.Principal;
import java.util.function.Supplier;

@RequestScoped
@Alternative
@Priority(1)
public class QuarkusPrincipalSupplier implements Supplier<Principal> {
  private final SecurityIdentity identity;

  @Inject
  public QuarkusPrincipalSupplier(SecurityIdentity identity) {
    this.identity = identity;
  }

  @Override
  public Principal get() {
    return identity.getPrincipal();
  }
}
