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

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import org.projectnessie.api.v1.http.HttpNamespaceApi;
import org.projectnessie.api.v1.params.MultipleNamespacesParams;
import org.projectnessie.api.v1.params.NamespaceParams;
import org.projectnessie.api.v1.params.NamespaceUpdate;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.spi.NamespaceService;

/** REST endpoint for the namespace-API. */
@RequestScoped
public class RestNamespaceResource implements HttpNamespaceApi {
  // Cannot extend the NamespaceApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpNamespaceApi or the plain NamespaceApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpNamespaceApi +
  // NamespaceApi, empty resources (no REST methods defined) and potentially other.

  private final NamespaceService namespaceService;

  // Mandated by CDI 2.0
  public RestNamespaceResource() {
    this(null);
  }

  @Inject
  public RestNamespaceResource(NamespaceService namespaceService) {
    this.namespaceService = namespaceService;
  }

  private NamespaceService resource() {
    return namespaceService;
  }

  @Override
  @JsonView(Views.V1.class)
  public Namespace createNamespace(NamespaceParams params, Namespace namespace)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    return resource().createNamespace(params.getRefName(), namespace);
  }

  @Override
  @JsonView(Views.V1.class)
  public void deleteNamespace(@NotNull NamespaceParams params)
      throws NessieReferenceNotFoundException,
          NessieNamespaceNotEmptyException,
          NessieNamespaceNotFoundException {
    resource().deleteNamespace(params.getRefName(), params.getNamespace());
  }

  @Override
  @JsonView(Views.V1.class)
  public Namespace getNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    return resource()
        .getNamespace(params.getRefName(), params.getHashOnRef(), params.getNamespace());
  }

  @Override
  @JsonView(Views.V1.class)
  public GetNamespacesResponse getNamespaces(@NotNull MultipleNamespacesParams params)
      throws NessieReferenceNotFoundException {
    return resource()
        .getNamespaces(params.getRefName(), params.getHashOnRef(), params.getNamespace());
  }

  @Override
  @JsonView(Views.V1.class)
  public void updateProperties(NamespaceParams params, NamespaceUpdate namespaceUpdate)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    resource()
        .updateProperties(
            params.getRefName(),
            params.getNamespace(),
            namespaceUpdate.getPropertyUpdates(),
            namespaceUpdate.getPropertyRemovals());
  }
}
