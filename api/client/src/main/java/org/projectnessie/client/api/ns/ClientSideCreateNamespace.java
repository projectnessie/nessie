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
package org.projectnessie.client.api.ns;

import static org.projectnessie.error.ContentKeyErrorDetails.contentKeyErrorDetails;

import java.util.Map;
import org.projectnessie.client.api.CreateNamespaceResult;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.builder.BaseCreateNamespaceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;

/**
 * Supports previous "create namespace" functionality of the java client over Nessie API v2.
 *
 * <p>API v2 does not have methods dedicated to manging namespaces. Namespaces are expected to be
 * managed as ordinary content objects.
 */
public final class ClientSideCreateNamespace extends BaseCreateNamespaceBuilder {
  private final NessieApiV2 api;

  public ClientSideCreateNamespace(NessieApiV2 api) {
    this.api = api;
  }

  @Override
  public Namespace create()
      throws NessieReferenceNotFoundException, NessieNamespaceAlreadyExistsException {
    return createWithResponse().getNamespace();
  }

  @Override
  public CreateNamespaceResult createWithResponse()
      throws NessieReferenceNotFoundException, NessieNamespaceAlreadyExistsException {
    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("Creating empty namespaces is not supported");
    }

    Namespace content = Namespace.builder().from(namespace).properties(properties).build();
    ContentKey key = namespace.toContentKey();

    GetMultipleContentsResponse contentsResponse;
    try {
      contentsResponse =
          api.getContent().refName(refName).hashOnRef(hashOnRef).key(key).getWithResponse();
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    Map<ContentKey, Content> contentMap = contentsResponse.toContentsMap();
    Content existing = contentMap.get(key);
    if (existing != null) {
      if (existing instanceof Namespace) {
        throw new NessieNamespaceAlreadyExistsException(
            contentKeyErrorDetails(key),
            String.format("Namespace '%s' already exists", key.toCanonicalString()));
      } else {
        throw new NessieNamespaceAlreadyExistsException(
            contentKeyErrorDetails(key),
            String.format(
                "Another content object with name '%s' already exists", key.toCanonicalString()));
      }
    }

    try {
      Branch branch = (Branch) contentsResponse.getEffectiveReference();

      CommitMeta meta = commitMeta;
      if (meta == null) {
        meta = CommitMeta.fromMessage("create namespace " + key);
      } else if (meta.getMessage().isEmpty()) {
        meta = CommitMeta.builder().from(meta).message("update namespace " + key).build();
      }

      CommitResponse committed =
          api.commitMultipleOperations()
              .commitMeta(meta)
              .branch(branch)
              .operation(Put.of(key, content))
              .commitWithResponse();

      Namespace created = content.withId(committed.toAddedContentsMap().get(key));
      return CreateNamespaceResult.of(created, committed.getTargetBranch());
    } catch (NessieNotFoundException | NessieConflictException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }
}
