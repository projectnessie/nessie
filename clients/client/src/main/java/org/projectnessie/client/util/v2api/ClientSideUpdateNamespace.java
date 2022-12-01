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

import java.util.HashMap;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.builder.BaseUpdateNamespaceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;

/**
 * Supports previous "update namespace" functionality of the java client over Nessie API v2.
 *
 * <p>API v2 does not have methods dedicated to manging namespaces. Namespaces are expected to be
 * managed as ordinary content objects.
 */
public final class ClientSideUpdateNamespace extends BaseUpdateNamespaceBuilder {
  private final NessieApiV2 api;

  public ClientSideUpdateNamespace(NessieApiV2 api) {
    this.api = api;
  }

  @Override
  public void update() throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    ContentKey key = ContentKey.of(namespace.getElements());
    Namespace oldNamespace =
        api.getNamespace().refName(refName).hashOnRef(hashOnRef).namespace(namespace).get();

    HashMap<String, String> newProperties = new HashMap<>(oldNamespace.getProperties());
    propertyRemovals.forEach(newProperties::remove);
    newProperties.putAll(propertyUpdates);

    ImmutableNamespace.Builder builder =
        ImmutableNamespace.builder().from(oldNamespace).properties(newProperties);

    try {
      api.commitMultipleOperations()
          .branchName(refName)
          .hash(hashOnRef)
          .commitMeta(CommitMeta.fromMessage("update namespace " + key))
          .operation(Operation.Put.of(key, builder.build()))
          .commit();
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    } catch (NessieConflictException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
