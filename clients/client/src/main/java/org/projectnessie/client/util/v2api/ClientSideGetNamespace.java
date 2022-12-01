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
import org.projectnessie.client.builder.BaseGetNamespaceBuilder;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;

/**
 * Supports previous "get namespace" functionality of the java client over Nessie API v2.
 *
 * <p>API v2 does not have methods dedicated to manging namespaces. Namespaces are expected to be
 * managed as ordinary content objects.
 */
public final class ClientSideGetNamespace extends BaseGetNamespaceBuilder {
  private final NessieApiV2 api;

  public ClientSideGetNamespace(NessieApiV2 api) {
    this.api = api;
  }

  @Override
  public Namespace get() throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    ContentKey key = ContentKey.of(namespace.getElements());
    Map<ContentKey, Content> contentMap;
    try {
      contentMap = api.getContent().refName(refName).hashOnRef(hashOnRef).key(key).get();

    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    Optional<Namespace> result =
        Optional.ofNullable(contentMap.get(key)).flatMap(c -> c.unwrap(Namespace.class));
    if (!result.isPresent()) {
      throw new NessieNamespaceNotFoundException(
          String.format("Namespace '%s' does not exist", key.toPathString()));
    }

    return result.get();
  }
}
