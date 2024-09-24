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

import static java.util.Collections.singleton;
import static org.projectnessie.services.authz.Check.canReadContentKey;
import static org.projectnessie.services.authz.Check.canViewReference;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthzPaginationIterator;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.hash.HashValidator;
import org.projectnessie.services.hash.ResolvedHash;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.KeyRestrictions;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.paging.PaginationIterator;

public class DiffApiImpl extends BaseApiImpl implements DiffService {

  public DiffApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext,
      ApiContext apiContext) {
    super(config, store, authorizer, accessContext, apiContext);
  }

  @Override
  public <R> R getDiff(
      String fromRef,
      String fromHash,
      String toRef,
      String toHash,
      String pagingToken,
      PagedResponseHandler<R, DiffEntry> pagedResponseHandler,
      Consumer<WithHash<NamedRef>> fromReference,
      Consumer<WithHash<NamedRef>> toReference,
      ContentKey minKey,
      ContentKey maxKey,
      ContentKey prefixKey,
      List<ContentKey> requestedKeys,
      String filter)
      throws NessieNotFoundException {

    try {
      ResolvedHash from =
          getHashResolver().resolveHashOnRef(fromRef, fromHash, new HashValidator("\"From\" hash"));
      ResolvedHash to =
          getHashResolver().resolveHashOnRef(toRef, toHash, new HashValidator("\"To\" hash"));
      NamedRef fromNamedRef = from.getNamedRef();
      NamedRef toNamedRef = to.getNamedRef();
      fromReference.accept(from);
      toReference.accept(to);

      startAccessCheck()
          .canViewReference(fromNamedRef)
          .canViewReference(toNamedRef)
          .checkAndThrow();

      BiPredicate<ContentKey, Content.Type> contentKeyPredicate = null;
      if (requestedKeys != null && !requestedKeys.isEmpty()) {
        Set<ContentKey> requestedKeysSet = new HashSet<>(requestedKeys);
        contentKeyPredicate = (key, type) -> requestedKeysSet.contains(key);
      }
      if (!Strings.isNullOrEmpty(filter)) {
        Predicate<ContentKey> contentKeyFilter = filterOnContentKey(filter);
        BiPredicate<ContentKey, Content.Type> filterPredicate =
            (key, type) -> contentKeyFilter.test(key);
        contentKeyPredicate =
            contentKeyPredicate != null
                ? contentKeyPredicate.and(filterPredicate)
                : filterPredicate;
      }

      try (PaginationIterator<Diff> diffs =
          getStore()
              .getDiffs(
                  from.getHash(),
                  to.getHash(),
                  pagingToken,
                  KeyRestrictions.builder()
                      .minKey(minKey)
                      .maxKey(maxKey)
                      .prefixKey(prefixKey)
                      .contentKeyPredicate(contentKeyPredicate)
                      .build())) {

        AuthzPaginationIterator<Diff> authz =
            new AuthzPaginationIterator<Diff>(
                diffs, super::startAccessCheck, getServerConfig().accessChecksBatchSize()) {
              @Override
              protected Set<Check> checksForEntry(Diff entry) {
                if (entry.getFromValue().isPresent()) {
                  if (entry.getToValue().isPresent()
                      && !Objects.equals(entry.getFromKey(), entry.getToKey())) {
                    return ImmutableSet.of(
                        canReadContentKey(fromNamedRef, entry.getFromKey()),
                        canReadContentKey(fromNamedRef, entry.getToKey()));
                  } else {
                    return singleton(canReadContentKey(fromNamedRef, entry.getFromKey()));
                  }
                } else {
                  return singleton(canReadContentKey(toNamedRef, entry.getToKey()));
                }
              }
            }.initialCheck(canViewReference(fromNamedRef))
                .initialCheck(canViewReference(toNamedRef));

        while (authz.hasNext()) {
          Diff diff = authz.next();
          ContentKey key = diff.contentKey();

          DiffEntry entry =
              DiffEntry.diffEntry(
                  key, diff.getFromValue().orElse(null), diff.getToValue().orElse(null));

          if (!pagedResponseHandler.addEntry(entry)) {
            pagedResponseHandler.hasMore(authz.tokenForCurrent());
            break;
          }
        }

        return pagedResponseHandler.build();
      }

    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }
}
