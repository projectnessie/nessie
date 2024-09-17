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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.Detached;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.hash.HashValidator;
import org.projectnessie.services.hash.ResolvedHash;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

public class ContentApiImpl extends BaseApiImpl implements ContentService {

  public ContentApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext,
      ApiContext apiContext) {
    super(config, store, authorizer, accessContext, apiContext);
  }

  @Override
  public ContentResponse getContent(
      ContentKey key,
      String namedRef,
      String hashOnRef,
      boolean withDocumentation,
      RequestMeta requestMeta)
      throws NessieNotFoundException {
    try {
      ResolvedHash ref =
          getHashResolver()
              .resolveHashOnRef(namedRef, hashOnRef, new HashValidator("Expected hash"));
      boolean forWrite = requestMeta.forWrite();
      ContentResult obj = getStore().getValue(ref.getHash(), key, forWrite);
      BatchAccessChecker accessCheck = startAccessCheck();

      NamedRef r = ref.getValue();
      accessCheck.canViewReference(r);
      if (forWrite) {
        accessCheck.canCommitChangeAgainstReference(r);
      }

      Set<String> actions = requestMeta.keyActions(key);
      if (obj != null && obj.content() != null) {
        accessCheck.canReadEntityValue(r, obj.identifiedKey(), actions);
        if (forWrite) {
          accessCheck.canUpdateEntity(r, obj.identifiedKey(), actions);
        }

        accessCheck.checkAndThrow();

        return ContentResponse.of(obj.content(), makeReference(ref), null);
      }

      if (forWrite) {
        accessCheck
            .canReadEntityValue(r, requireNonNull(obj, "obj is null").identifiedKey(), actions)
            .canCreateEntity(r, obj.identifiedKey(), actions);
      }
      accessCheck.checkAndThrow();

      throw new NessieContentNotFoundException(key, namedRef);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public GetMultipleContentsResponse getMultipleContents(
      String namedRef,
      String hashOnRef,
      List<ContentKey> keys,
      boolean withDocumentation,
      RequestMeta requestMeta)
      throws NessieNotFoundException {
    try {
      ResolvedHash ref =
          getHashResolver()
              .resolveHashOnRef(namedRef, hashOnRef, new HashValidator("Expected hash"));

      NamedRef r = ref.getValue();
      BatchAccessChecker check = startAccessCheck().canViewReference(r);
      boolean forWrite = requestMeta.forWrite();
      if (forWrite) {
        check.canCommitChangeAgainstReference(r);
      }

      Map<ContentKey, ContentResult> values = getStore().getValues(ref.getHash(), keys, forWrite);
      List<ContentWithKey> output =
          values.entrySet().stream()
              .filter(
                  e -> {
                    ContentResult contentResult = e.getValue();
                    IdentifiedContentKey identifiedKey = contentResult.identifiedKey();
                    Set<String> actions = requestMeta.keyActions(identifiedKey.contentKey());
                    check.canReadEntityValue(r, identifiedKey, actions);
                    if (contentResult.content() != null) {
                      if (forWrite) {
                        check.canUpdateEntity(r, identifiedKey, actions);
                      }
                      return true;
                    } else {
                      if (forWrite) {
                        check.canCreateEntity(r, identifiedKey, actions);
                      }
                      return false;
                    }
                  })
              .map(
                  e -> {
                    ContentResult contentResult = e.getValue();
                    return ContentWithKey.of(
                        e.getKey(), contentResult.content(), contentResult.documentation());
                  })
              .collect(Collectors.toList());

      check.checkAndThrow();

      return GetMultipleContentsResponse.of(output, makeReference(ref));
    } catch (ReferenceNotFoundException ex) {
      throw new NessieReferenceNotFoundException(ex.getMessage(), ex);
    }
  }

  private static Reference makeReference(WithHash<NamedRef> refWithHash) {
    NamedRef ref = refWithHash.getValue();
    if (ref instanceof TagName) {
      return Tag.of(ref.getName(), refWithHash.getHash().asString());
    } else if (ref instanceof BranchName) {
      return Branch.of(ref.getName(), refWithHash.getHash().asString());
    } else if (ref instanceof DetachedRef) {
      return Detached.of(refWithHash.getHash().asString());
    } else {
      throw new UnsupportedOperationException("only converting tags or branches"); // todo
    }
  }
}
