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
import java.util.Optional;
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

abstract class BaseResource {
  private final ServerConfig config;

  private final VersionStore<Contents, CommitMeta, Contents.Type> store;

  // Mandated by CDI 2.0
  protected BaseResource() {
    this(null, null);
  }

  protected BaseResource(
      ServerConfig config, VersionStore<Contents, CommitMeta, Contents.Type> store) {
    this.config = config;
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

  protected VersionStore<Contents, CommitMeta, Contents.Type> getStore() {
    return store;
  }

  protected abstract SecurityContext getSecurityContext();

  protected Principal getPrincipal() {
    return null == getSecurityContext() ? null : getSecurityContext().getUserPrincipal();
  }
}
