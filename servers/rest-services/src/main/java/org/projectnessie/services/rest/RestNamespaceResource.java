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
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.api.NamespaceApi;
import org.projectnessie.api.http.HttpNamespaceApi;
import org.projectnessie.api.params.MultipleNamespacesParams;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.NamespaceApiImplWithAuthorization;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the namespace-API. */
@RequestScoped
public class RestNamespaceResource implements HttpNamespaceApi {
  // Cannot extend the NamespaceApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpNamespaceApi or the plain NamespaceApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpNamespaceApi +
  // NamespaceApi, empty resources (no REST methods defined) and potentially other.

  private final ServerConfig config;
  private final VersionStore<Content, CommitMeta, Type> store;
  private final Authorizer authorizer;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestNamespaceResource() {
    this(null, null, null);
  }

  @Inject
  public RestNamespaceResource(
      ServerConfig config, VersionStore<Content, CommitMeta, Type> store, Authorizer authorizer) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
  }

  private NamespaceApi resource() {
    return new NamespaceApiImplWithAuthorization(
        config,
        store,
        authorizer,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public Namespace createNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    return resource().createNamespace(params);
  }

  @Override
  public void deleteNamespace(@NotNull NamespaceParams params)
      throws NessieReferenceNotFoundException, NessieNamespaceNotEmptyException,
          NessieNamespaceNotFoundException {
    resource().deleteNamespace(params);
  }

  @Override
  public Namespace getNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    return resource().getNamespace(params);
  }

  @Override
  public GetNamespacesResponse getNamespaces(@NotNull MultipleNamespacesParams params)
      throws NessieReferenceNotFoundException {
    return resource().getNamespaces(params);
  }
}
