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
import org.projectnessie.api.ContentApi;
import org.projectnessie.api.http.HttpContentApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MultiGetContentRequest;
import org.projectnessie.model.MultiGetContentResponse;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ContentApiImplWithAuthorization;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the content-API. */
@RequestScoped
public class RestContentResource implements HttpContentApi {
  // Cannot extend the ContentApiImplWithAuthn class, because then CDI gets confused
  // about which interface to use - either HttpContentApi or the plain ContentApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final ServerConfig config;
  private final VersionStore<Content, CommitMeta, Type> store;
  private final AccessChecker accessChecker;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestContentResource() {
    this(null, null, null);
  }

  @Inject
  public RestContentResource(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Type> store,
      AccessChecker accessChecker) {
    this.config = config;
    this.store = store;
    this.accessChecker = accessChecker;
  }

  private ContentApi resource() {
    return new ContentApiImplWithAuthorization(
        config,
        store,
        accessChecker,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public Content getContent(ContentKey key, String ref, String hashOnRef)
      throws NessieNotFoundException {
    return resource().getContent(key, ref, hashOnRef);
  }

  @Override
  public MultiGetContentResponse getMultipleContents(
      String ref, String hashOnRef, MultiGetContentRequest request) throws NessieNotFoundException {
    return resource().getMultipleContents(ref, hashOnRef, request);
  }
}
