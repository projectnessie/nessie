/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api;

import javax.validation.constraints.NotNull;
import org.projectnessie.api.params.MultipleNamespacesParams;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.api.params.NamespaceUpdate;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;

public interface NamespaceApi {

  /**
   * Creates a new namespace with Namespace properties.
   *
   * @param params The {@link NamespaceParams} that includes the parameters for the API call.
   * @param namespace The instance including the namespace properties.
   * @return A {@link Namespace} instance if creating the namespace succeeded.
   * @throws NessieReferenceNotFoundException If the reference could not be found.
   * @throws NessieNamespaceAlreadyExistsException If the namespace already exists.
   */
  Namespace createNamespace(@NotNull NamespaceParams params, @NotNull Namespace namespace)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException;

  /**
   * Deletes the namespace if it doesn't contain any tables.
   *
   * @param params The {@link NamespaceParams} that includes the parameters for the API call.
   * @throws NessieReferenceNotFoundException If the reference could not be found.
   * @throws NessieNamespaceNotEmptyException If the namespace is not empty and contains tables.
   * @throws NessieNamespaceNotFoundException If the namespace to be deleted could not be found.
   */
  void deleteNamespace(@NotNull NamespaceParams params)
      throws NessieReferenceNotFoundException, NessieNamespaceNotEmptyException,
          NessieNamespaceNotFoundException;

  /**
   * Retrieves the {@link Namespace} instance if it exists.
   *
   * @param params The {@link NamespaceParams} that includes the parameters for the API call.
   * @return A {@link Namespace} instance if a namespace with the given name exists.
   * @throws NessieReferenceNotFoundException If the reference could not be found.
   * @throws NessieNamespaceNotFoundException If the namespace does not exist.
   */
  Namespace getNamespace(@NotNull NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException;

  /**
   * Retrieves a list of {@link Namespace} instances that match a given namespace prefix.
   *
   * @param params The {@link MultipleNamespacesParams} that includes the parameters for the API
   *     call.
   * @return A {@link GetNamespacesResponse} instance containing all the namespaces that match the
   *     given namespace prefix.
   * @throws NessieReferenceNotFoundException If the reference could not be found.
   */
  GetNamespacesResponse getNamespaces(@NotNull MultipleNamespacesParams params)
      throws NessieReferenceNotFoundException;

  /**
   * Updates/removes properties for a given {@link Namespace}.
   *
   * @param params The {@link NamespaceParams} that includes the parameters for the API call.
   * @param namespaceUpdate The instance including the property updates/deletes.
   * @throws NessieNamespaceNotFoundException If the namespace does not exist.
   * @throws NessieReferenceNotFoundException If the reference could not be found.
   */
  void updateProperties(@NotNull NamespaceParams params, @NotNull NamespaceUpdate namespaceUpdate)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException;
}
