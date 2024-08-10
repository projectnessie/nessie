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

import org.projectnessie.client.api.DeleteNamespaceResult;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.builder.BaseDeleteNamespaceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Reference;

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
      throws NessieNamespaceNotFoundException,
          NessieReferenceNotFoundException,
          NessieNamespaceNotEmptyException {
    deleteWithResponse();
  }

  @Override
  public DeleteNamespaceResult deleteWithResponse()
      throws NessieNamespaceNotFoundException,
          NessieReferenceNotFoundException,
          NessieNamespaceNotEmptyException {
    ContentKey key = namespace.toContentKey();
    Content existing;
    Reference ref;
    try {
      ContentResponse contentResponse =
          api.getContent().refName(refName).hashOnRef(hashOnRef).getSingle(key);
      ref = contentResponse.getEffectiveReference();
      if (!(ref instanceof Branch)) {
        throw new NessieReferenceNotFoundException(
            "Must only commit against a branch, but got " + ref);
      }
      existing = contentResponse.getContent();
    } catch (NessieContentNotFoundException e) {
      existing = null;
      ref = null;
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    if (!(existing instanceof Namespace)) {
      throw new NessieNamespaceNotFoundException(
          contentKeyErrorDetails(key),
          String.format("Namespace '%s' does not exist", key.toCanonicalString()));
    }

    try {
      if (api
          .getEntries()
          .reference(ref)
          .filter(String.format("entry.encodedKey.startsWith('%s.')", namespace.toPathString()))
          .stream()
          .findAny()
          .isPresent()) {
        throw new NessieNamespaceNotEmptyException(
            contentKeyErrorDetails(key),
            String.format("Namespace '%s' is not empty", key.toCanonicalString()));
      }
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    try {
      CommitMeta meta = commitMeta;
      if (meta == null) {
        meta = CommitMeta.fromMessage("delete namespace " + key);
      } else if (meta.getMessage().isEmpty()) {
        meta = CommitMeta.builder().from(meta).message("delete namespace " + key).build();
      }

      CommitResponse commit =
          api.commitMultipleOperations()
              .branch((Branch) ref)
              .commitMeta(meta)
              .operation(Delete.of(key))
              .commitWithResponse();

      return DeleteNamespaceResult.of(namespace, commit.getTargetBranch());
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (NessieConflictException e) {
      throw new IllegalStateException(e);
    }
  }
}
