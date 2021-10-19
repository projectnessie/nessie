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
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.http.HttpContentsApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ContentsApiImplWithAuthorization;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the contents-API. */
@RequestScoped
public class RestContentsResource implements HttpContentsApi {
  // Cannot extend the ContentsApiImplWithAuthn class, because then CDI gets confused
  // about which interface to use - either HttpContentsApi or the plain ContentsApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final ServerConfig config;
  private final VersionStore<Contents, CommitMeta, Type> store;
  private final AccessChecker accessChecker;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestContentsResource() {
    this(null, null, null);
  }

  @Inject
  public RestContentsResource(
      ServerConfig config,
      VersionStore<Contents, CommitMeta, Type> store,
      AccessChecker accessChecker) {
    this.config = config;
    this.store = store;
    this.accessChecker = accessChecker;
  }

  private ContentsApi resource() {
    return new ContentsApiImplWithAuthorization(
        config,
        store,
        accessChecker,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public Contents getContents(ContentsKey key, String ref, String hashOnRef)
      throws NessieNotFoundException {
    return resource().getContents(key, ref, hashOnRef);
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(
      String ref, String hashOnRef, MultiGetContentsRequest request)
      throws NessieNotFoundException {
    return resource().getMultipleContents(ref, hashOnRef, request);
  }
}
