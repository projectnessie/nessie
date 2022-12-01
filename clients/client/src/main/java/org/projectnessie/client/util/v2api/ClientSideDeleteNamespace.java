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
package org.projectnessie.client.util.v2api;

import java.util.Map;
import java.util.Optional;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.builder.BaseDeleteNamespaceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;

/**
 * Supports previous "delete namespace" functionality of the java client over Nessie API v2.
 *
 * <p>API v2 does not have methods dedicated to manging namespaces. Namespaces are expected to be
 * managed as ordinary content objects.
 */
public final class ClientSideDeleteNamespace extends BaseDeleteNamespaceBuilder {
  private final NessieApiV2 api;

  public ClientSideDeleteNamespace(NessieApiV2 api) {
    this.api = api;
  }

  @Override
  public void delete()
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException,
          NessieNamespaceNotEmptyException {
    ContentKey key = ContentKey.of(namespace.getElements());
    Map<ContentKey, Content> contentMap;
    try {
      contentMap = api.getContent().refName(refName).hashOnRef(hashOnRef).key(key).get();
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    Optional<Object> existingNamespace =
        Optional.ofNullable(contentMap.get(key)).flatMap(c -> c.unwrap(Namespace.class));

    if (!existingNamespace.isPresent()) {
      throw new NessieNamespaceNotFoundException(
          String.format("Namespace '%s' does not exist", key.toPathString()));
    }

    Optional<EntriesResponse.Entry> entry;
    try {
      entry =
          api
              .getEntries()
              .refName(refName)
              .hashOnRef(hashOnRef)
              .filter(String.format("entry.namespace.startsWith('%s')", key))
              .stream()
              .findAny();
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    if (entry.isPresent()) {
      throw new NessieNamespaceNotEmptyException(
          String.format("Namespace '%s' is not empty", key.toPathString()));
    }

    try {
      api.commitMultipleOperations()
          .branchName(refName)
          .hash(hashOnRef)
          .commitMeta(CommitMeta.fromMessage("delete namespace " + key))
          .operation(Operation.Delete.of(key))
          .commit();
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (NessieConflictException e) {
      throw new IllegalStateException(e);
    }
  }
}
