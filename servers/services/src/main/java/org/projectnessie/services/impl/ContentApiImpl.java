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
package org.projectnessie.services.impl;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.api.ContentApi;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.ImmutableGetMultipleContentsResponse;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

public class ContentApiImpl extends BaseApiImpl implements ContentApi {

  public ContentApiImpl(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      AccessChecker accessChecker,
      Principal principal) {
    super(config, store, accessChecker, principal);
  }

  @Override
  public Content getContent(ContentKey key, String namedRef, String hashOnRef)
      throws NessieNotFoundException {
    WithHash<NamedRef> ref = namedRefWithHashOrThrow(namedRef, hashOnRef);
    try {
      Content obj = getStore().getValue(ref.getHash(), toKey(key));
      if (obj != null) {
        return obj;
      }
      throw new NessieContentNotFoundException(key, namedRef);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public GetMultipleContentsResponse getMultipleContents(
      String namedRef, String hashOnRef, GetMultipleContentsRequest request)
      throws NessieNotFoundException {
    try {
      WithHash<NamedRef> ref = namedRefWithHashOrThrow(namedRef, hashOnRef);
      List<ContentKey> externalKeys = request.getRequestedKeys();
      List<Key> internalKeys =
          externalKeys.stream().map(ContentApiImpl::toKey).collect(Collectors.toList());
      Map<Key, Content> values = getStore().getValues(ref.getHash(), internalKeys);
      List<ContentWithKey> output =
          values.entrySet().stream()
              .map(e -> ContentWithKey.of(toContentKey(e.getKey()), e.getValue()))
              .collect(Collectors.toList());

      return ImmutableGetMultipleContentsResponse.builder().contents(output).build();
    } catch (ReferenceNotFoundException ex) {
      throw new NessieReferenceNotFoundException(ex.getMessage(), ex);
    }
  }

  static Key toKey(ContentKey key) {
    return Key.of(key.getElements().toArray(new String[0]));
  }

  static ContentKey toContentKey(Key key) {
    return ContentKey.of(key.getElements());
  }
}
