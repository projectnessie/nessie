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
package org.projectnessie.client.http;

import javax.validation.constraints.NotNull;
import org.projectnessie.api.http.HttpNamespaceApi;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.api.params.NamespacesParams;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;

class HttpNamespaceClient implements HttpNamespaceApi {

  private final HttpClient client;

  public HttpNamespaceClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public Namespace createNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    return client
        .newRequest()
        .path("namespaces/namespace/{ref}/{name}")
        .resolveTemplate("ref", params.getRefName())
        .resolveTemplate("name", params.getNamespace().toPathString())
        .queryParam("hashOnRef", params.getHashOnRef())
        .put(params.getNamespace())
        .readEntity(Namespace.class);
  }

  @Override
  public void deleteNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieNamespaceNotEmptyException,
          NessieReferenceNotFoundException {
    client
        .newRequest()
        .path("namespaces/namespace/{ref}/{name}")
        .resolveTemplate("ref", params.getRefName())
        .resolveTemplate("name", params.getNamespace().toPathString())
        .delete();
  }

  @Override
  public Namespace getNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    return client
        .newRequest()
        .path("namespaces/namespace/{ref}/{name}")
        .resolveTemplate("ref", params.getRefName())
        .resolveTemplate("name", params.getNamespace().toPathString())
        .queryParam("hashOnRef", params.getHashOnRef())
        .get()
        .readEntity(Namespace.class);
  }

  @Override
  public GetNamespacesResponse getNamespaces(@NotNull NamespacesParams params)
      throws NessieReferenceNotFoundException {
    return client
        .newRequest()
        .path("namespaces/{ref}")
        .resolveTemplate("ref", params.getRefName())
        .queryParam(
            "name", null != params.getNamespace() ? params.getNamespace().toPathString() : null)
        .queryParam("hashOnRef", params.getHashOnRef())
        .get()
        .readEntity(GetNamespacesResponse.class);
  }
}
