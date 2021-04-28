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
package org.projectnessie.services.rest;

import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

abstract class BaseResource {
  private final ServerConfig config;

  private final Principal principal;

  private final VersionStore<Contents, CommitMeta, Contents.Type> store;

  // Mandated by CDI 2.0
  protected BaseResource() {
    this(null, null, null);
  }

  protected BaseResource(ServerConfig config, Principal principal, VersionStore<Contents, CommitMeta, Contents.Type> store) {
    this.config = config;
    this.principal = principal;
    this.store = store;
  }

  Optional<Hash> getHash(String ref) {
    try {
      WithHash<Ref> whr = store.toRef(Optional.ofNullable(ref).orElse(config.getDefaultBranch()));
      return Optional.of(whr.getHash());
    } catch (ReferenceNotFoundException e) {
      return Optional.empty();
    }
  }

  Hash getHashOrThrow(String ref) throws NessieNotFoundException {
    return getHash(ref).orElseThrow(() -> new NessieNotFoundException(String.format("Ref for %s not found", ref)));
  }

  protected ServerConfig getConfig() {
    return config;
  }

  protected VersionStore<Contents, CommitMeta, Contents.Type> getStore() {
    return store;
  }

  protected Hash doOps(String branch, String hash, CommitMeta commitMeta, List<Operation<Contents>> operations)
      throws NessieConflictException, NessieNotFoundException {
    try {
      return store.commit(
          BranchName.of(Optional.ofNullable(branch).orElse(config.getDefaultBranch())),
          Optional.ofNullable(hash).map(Hash::of),
          meta(principal, commitMeta),
          operations
      );
    } catch (IllegalArgumentException e) {
      throw new NessieNotFoundException("Invalid hash provided. " + e.getMessage(), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException("Failed to commit data. " + e.getMessage(), e);
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException("Failed to commit data. " + e.getMessage(), e);
    }
  }

  private static CommitMeta meta(Principal principal, CommitMeta commitMeta) throws NessieConflictException {
    if (commitMeta.getCommitter() != null) {
      throw new NessieConflictException("Cannot set the committer on the client side. It is set by the server.");
    }
    String committer = principal == null ? "" : principal.getName();
    Instant now = Instant.now();
    return commitMeta.toBuilder()
        .committer(committer)
        .commitTime(now)
        .author(commitMeta.getAuthor() == null ? committer : commitMeta.getAuthor())
        .authorTime(commitMeta.getAuthorTime() == null ? now : commitMeta.getAuthorTime())
        .build();
  }
}
