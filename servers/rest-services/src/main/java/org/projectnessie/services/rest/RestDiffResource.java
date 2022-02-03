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
import org.projectnessie.api.DiffApi;
import org.projectnessie.api.http.HttpDiffApi;
import org.projectnessie.api.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.DiffApiImplWithAuthorization;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the diff-API. */
@RequestScoped
public class RestDiffResource implements HttpDiffApi {
  // Cannot extend the DiffApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpContentApi or the plain ContentApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final ServerConfig config;
  private final VersionStore<Content, CommitMeta, Type> store;
  private final Authorizer authorizer;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestDiffResource() {
    this(null, null, null);
  }

  @Inject
  public RestDiffResource(
      ServerConfig config, VersionStore<Content, CommitMeta, Type> store, Authorizer authorizer) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
  }

  private DiffApi resource() {
    return new DiffApiImplWithAuthorization(
        config,
        store,
        authorizer,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    return resource().getDiff(params);
  }
}
