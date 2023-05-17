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
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.AuthzPaginationIterator;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.paging.PaginationIterator;

public class DiffApiImpl extends BaseApiImpl implements DiffService {

  public DiffApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    super(config, store, authorizer, principal);
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
    WithHash<NamedRef> from = namedRefWithHashOrThrow(fromRef, fromHash);
    WithHash<NamedRef> to = namedRefWithHashOrThrow(toRef, toHash);
    NamedRef fromNamedRef = from.getValue();
    NamedRef toNamedRef = to.getValue();
    fromReference.accept(from);
    toReference.accept(to);

    startAccessCheck().canViewReference(fromNamedRef).canViewReference(toNamedRef).checkAndThrow();

    try {
      Predicate<ContentKey> contentKeyPredicate = null;
      if (requestedKeys != null && !requestedKeys.isEmpty()) {
        contentKeyPredicate = new HashSet<>(requestedKeys)::contains;
      }
      if (!Strings.isNullOrEmpty(filter)) {
        Predicate<ContentKey> filterPredicate = filterOnContentKey(filter);
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
                  minKey,
                  maxKey,
                  prefixKey,
                  contentKeyPredicate)) {

        AuthzPaginationIterator<Diff> authz =
            new AuthzPaginationIterator<Diff>(
                diffs, super::startAccessCheck, ACCESS_CHECK_BATCH_SIZE) {
              @Override
              protected Set<Check> checksForEntry(Diff entry) {
                ContentKey key = ContentKey.of(entry.getKey().getElements());
                String fromContent = entry.getFromValue().map(Content::getId).orElse(null);
                String toContent = entry.getToValue().map(Content::getId).orElse(null);
                if (fromNamedRef.equals(toNamedRef)
                    && fromContent != null
                    && fromContent.equals(toContent)) {
                  return singleton(canReadContentKey(fromNamedRef, key, fromContent));
                } else {
                  if (fromContent != null && toContent != null) {
                    return ImmutableSet.of(
                        canReadContentKey(fromNamedRef, key, fromContent),
                        canReadContentKey(toNamedRef, key, toContent));
                  }
                  if (fromContent != null) {
                    return singleton(canReadContentKey(fromNamedRef, key, fromContent));
                  }
                  if (toContent != null) {
                    return singleton(canReadContentKey(toNamedRef, key, toContent));
                  }
                }
                return singleton(canReadContentKey(toNamedRef, key, null));
              }
            }.initialCheck(canViewReference(fromNamedRef))
                .initialCheck(canViewReference(toNamedRef));

        while (authz.hasNext()) {
          Diff diff = authz.next();
          ContentKey key = ContentKey.of(diff.getKey().getElements());

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
