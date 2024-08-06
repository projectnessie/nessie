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

import static java.lang.String.format;

import java.util.Objects;
import java.util.Optional;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetMultipleNamespacesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.builder.BaseGetMultipleNamespacesBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.ImmutableGetNamespacesResponse;
import org.projectnessie.model.Namespace;

/**
 * Supports previous "get multiple namespaces" functionality of the java client over Nessie API v2.
 *
 * <p>API v2 does not have methods dedicated to managing namespaces. Namespaces are expected to be
 * managed as ordinary content objects.
 */
public final class ClientSideGetMultipleNamespaces extends BaseGetMultipleNamespacesBuilder {
  private final NessieApiV2 api;
  private boolean onlyDirectChildren;

  public ClientSideGetMultipleNamespaces(NessieApiV2 api) {
    this.api = api;
  }

  @Override
  public GetMultipleNamespacesBuilder onlyDirectChildren(boolean onlyDirectChildren) {
    this.onlyDirectChildren = onlyDirectChildren;
    return this;
  }

  @Override
  public GetNamespacesResponse get() throws NessieReferenceNotFoundException {

    ImmutableGetNamespacesResponse.Builder builder = GetNamespacesResponse.builder();

    try {
      GetEntriesBuilder getEntries =
          api.getEntries().maxRecords(100).refName(refName).hashOnRef(hashOnRef).withContent(true);

      String filter = "entry.contentType == 'NAMESPACE'";
      if (namespace != null && !namespace.isEmpty()) {
        String nsName = namespace.toPathString();
        filter +=
            onlyDirectChildren
                ? format(
                    " && size(entry.keyElements) == %d && entry.encodedKey.startsWith('%s.')",
                    namespace.getElementCount() + 1, nsName)
                : format(
                    "&& (entry.encodedKey == '%s' || entry.encodedKey.startsWith('%s.'))",
                    nsName, nsName);
      } else if (onlyDirectChildren) {
        filter += " && size(entry.keyElements) == 1";
      }
      getEntries.filter(filter);

      EntriesResponse response;
      do {
        response = getEntries.get();
        builder.effectiveReference(response.getEffectiveReference());
        response.getEntries().stream()
            .map(EntriesResponse.Entry::getContent)
            .filter(Objects::nonNull) // always true in V2
            .map(v -> v.unwrap(Namespace.class))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(builder::addNamespaces);
        getEntries = getEntries.pageToken(response.getToken());
      } while (response.isHasMore());

    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    return builder.build();
  }
}
