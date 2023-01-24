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
import java.util.function.Supplier;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
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
      PagedResponseHandler<R, DiffEntry> pagedResponseHandler)
      throws NessieNotFoundException {
    WithHash<NamedRef> from = namedRefWithHashOrThrow(fromRef, fromHash);
    WithHash<NamedRef> to = namedRefWithHashOrThrow(toRef, toHash);
    NamedRef fromNamedRef = from.getValue();
    NamedRef toNamedRef = to.getValue();

    startAccessCheck().canViewReference(fromNamedRef).canViewReference(toNamedRef).checkAndThrow();

    try {
      try (PaginationIterator<Diff> diffs =
          getStore().getDiffs(from.getHash(), to.getHash(), pagingToken)) {
        while (diffs.hasNext()) {
          Diff diff = diffs.next();
          ContentKey key = ContentKey.of(diff.getKey().getElements());
          DiffEntry entry =
              DiffEntry.diffEntry(
                  key, diff.getFromValue().orElse(null), diff.getToValue().orElse(null));

          BatchAccessChecker check = startAccessCheck();
          Content fromContent = entry.getFrom();
          Content toContent = entry.getTo();
          if (fromNamedRef.equals(toNamedRef)
              && fromContent != null
              && toContent != null
              && fromContent.getId() != null
              && fromContent.getId().equals(toContent.getId())) {
            startAccessCheck().canReadContentKey(fromNamedRef, key, fromContent.getId());
          } else {
            if (fromContent != null) {
              startAccessCheck().canReadContentKey(fromNamedRef, key, fromContent.getId());
            }
            if (toContent != null) {
              startAccessCheck().canReadContentKey(toNamedRef, key, toContent.getId());
            }
          }

          if (!check.check().isEmpty()) {
            continue;
          }

          if (!pagedResponseHandler.addEntry(entry)) {
            pagedResponseHandler.hasMore(diffs.tokenForCurrent());
            break;
          }
        }
      }

    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
    return pagedResponseHandler.build();
  }
}
