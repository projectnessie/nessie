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
package org.projectnessie.services.rest;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.executable.ExecutableType;
import jakarta.validation.executable.ValidateOnExecution;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.NamespaceApiImpl;
import org.projectnessie.versioned.VersionStore;

@RequestScoped
@ValidateOnExecution(type = ExecutableType.ALL)
public class RestNamespaceService extends NamespaceApiImpl {
  // Mandated by CDI 2.0
  public RestNamespaceService() {
    this(null, null, null, null);
  }

  @Inject
  public RestNamespaceService(
      ServerConfig config, VersionStore store, Authorizer authorizer, AccessContext accessContext) {
    super(config, store, authorizer, accessContext);
  }
}
