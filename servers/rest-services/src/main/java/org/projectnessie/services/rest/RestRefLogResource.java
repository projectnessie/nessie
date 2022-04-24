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
package org.projectnessie.services.rest;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.api.RefLogApi;
import org.projectnessie.api.http.HttpRefLogApi;
import org.projectnessie.api.params.RefLogParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.RefLogApiImplWithAuthorization;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the reflog-API. */
@RequestScoped
public class RestRefLogResource implements HttpRefLogApi {

  private final ServerConfig config;
  private final VersionStore<Content, CommitMeta, Content.Type> store;
  private final Authorizer authorizer;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestRefLogResource() {
    this(null, null, null);
  }

  @Inject
  public RestRefLogResource(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      Authorizer authorizer) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
  }

  private RefLogApi resource() {
    return new RefLogApiImplWithAuthorization(
        config,
        store,
        authorizer,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public RefLogResponse getRefLog(RefLogParams params) throws NessieNotFoundException {
    return resource().getRefLog(params);
  }
}
