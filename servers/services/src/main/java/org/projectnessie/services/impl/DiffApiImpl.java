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
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.PaginationIterator;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

public class DiffApiImpl extends BaseApiImpl implements DiffService {

  public DiffApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public <B, R> R getDiff(
      String fromRef,
      String fromHash,
      String toRef,
      String toHash,
      String pagingToken,
      PagedResponseHandler<B, R, DiffEntry> pagedResponseHandler)
      throws NessieNotFoundException {
    WithHash<NamedRef> from = namedRefWithHashOrThrow(fromRef, fromHash);
    WithHash<NamedRef> to = namedRefWithHashOrThrow(toRef, toHash);
    return getDiff(from.getHash(), to.getHash(), pagingToken, pagedResponseHandler);
  }

  protected <B, R> R getDiff(
      Hash from,
      Hash to,
      String pagingToken,
      PagedResponseHandler<B, R, DiffEntry> pagedResponseHandler)
      throws NessieNotFoundException {
    B builder = pagedResponseHandler.newBuilder();
    try {
      try (PaginationIterator<Diff> diffs = getStore().getDiffs(from, to, pagingToken)) {
        int cnt = 0;
        while (diffs.hasNext()) {
          Diff diff = diffs.next();
          if (!pagedResponseHandler.addEntry(
              builder,
              ++cnt,
              DiffEntry.diffEntry(
                  ContentKey.of(diff.getKey().getElements()),
                  diff.getFromValue().orElse(null),
                  diff.getToValue().orElse(null)))) {
            pagedResponseHandler.hasMore(builder, diffs.tokenForCurrent());
            break;
          }
        }
      }

    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
    return pagedResponseHandler.build(builder);
  }
}
