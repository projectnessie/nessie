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
package com.dremio.nessie.services.rest;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ImmutableCommitMeta;
import com.dremio.nessie.services.config.ServerConfig;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import java.security.Principal;
import java.util.List;
import java.util.Optional;

abstract class BaseResource {
  private final ServerConfig config;

  private final Principal principal;

  private final VersionStore<Contents, CommitMeta> store;

  // Mandated by CDI 2.0
  protected BaseResource() {
    this(null, null, null);
  }

  protected BaseResource(
      ServerConfig config, Principal principal, VersionStore<Contents, CommitMeta> store) {
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
    return getHash(ref)
        .orElseThrow(() -> new NessieNotFoundException(String.format("Ref for %s not found", ref)));
  }

  protected ServerConfig getConfig() {
    return config;
  }

  protected VersionStore<Contents, CommitMeta> getStore() {
    return store;
  }

  protected void doOps(
      String branch,
      String hash,
      String message,
      List<com.dremio.nessie.versioned.Operation<Contents>> operations)
      throws NessieConflictException, NessieNotFoundException {
    try {
      store.commit(
          BranchName.of(Optional.ofNullable(branch).orElse(config.getDefaultBranch())),
          Optional.ofNullable(hash).map(Hash::of),
          meta(principal, message),
          operations);
    } catch (IllegalArgumentException e) {
      throw new NessieNotFoundException("Invalid hash provided.", e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          "Failed to commit data. Provided hash does not match current value.", e);
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException("Failed to commit data. Provided ref was not found.", e);
    }
  }

  private static CommitMeta meta(Principal principal, String message) {
    return ImmutableCommitMeta.builder()
        .commiter(principal == null ? "" : principal.getName())
        .message(message == null ? "" : message)
        .commitTime(System.currentTimeMillis())
        .build();
  }
}
