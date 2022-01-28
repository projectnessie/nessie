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
import java.util.stream.Stream;
import org.projectnessie.api.DiffApi;
import org.projectnessie.api.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.ImmutableDiffEntry;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ImmutableDiffResponse.Builder;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

public class DiffApiImpl extends BaseApiImpl implements DiffApi {

  public DiffApiImpl(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      AccessChecker accessChecker,
      Principal principal) {
    super(config, store, accessChecker, principal);
  }

  @Override
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    WithHash<NamedRef> from =
        namedRefWithHashOrThrow(params.getFromRef(), params.getFromHashOnRef());
    WithHash<NamedRef> to = namedRefWithHashOrThrow(params.getToRef(), params.getToHashOnRef());
    return getDiff(from.getHash(), to.getHash());
  }

  protected DiffResponse getDiff(Hash from, Hash to) throws NessieNotFoundException {
    Builder builder = ImmutableDiffResponse.builder();
    try {
      try (Stream<Diff<Content>> diffs = getStore().getDiffs(from, to)) {
        diffs
            .map(
                diff ->
                    ImmutableDiffEntry.builder()
                        .key(ContentKey.of(diff.getKey().getElements()))
                        .from(diff.getFromValue().orElse(null))
                        .to(diff.getToValue().orElse(null))
                        .build())
            .forEach(builder::addDiffs);
      }

    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
    return builder.build();
  }
}
