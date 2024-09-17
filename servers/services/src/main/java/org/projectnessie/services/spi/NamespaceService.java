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
package org.projectnessie.services.spi;

import java.util.Map;
import java.util.Set;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.versioned.RequestMeta;

/**
 * Server-side interface to services managing namespaces.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface NamespaceService {

  Namespace createNamespace(String refName, Namespace namespace, RequestMeta requestMeta)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException;

  void updateProperties(
      String refName,
      Namespace namespaceToUpdate,
      Map<String, String> propertyUpdates,
      Set<String> propertyRemovals,
      RequestMeta requestMeta)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException;

  void deleteNamespace(String refName, Namespace namespaceToDelete)
      throws NessieReferenceNotFoundException,
          NessieNamespaceNotEmptyException,
          NessieNamespaceNotFoundException;

  Namespace getNamespace(String refName, String hashOnRef, Namespace namespace)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException;

  GetNamespacesResponse getNamespaces(String refName, String hashOnRef, Namespace namespace)
      throws NessieReferenceNotFoundException;
}
