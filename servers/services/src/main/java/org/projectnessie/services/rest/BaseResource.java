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
import java.util.UUID;
import javax.annotation.Nullable;
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.authz.ServerAccessContext;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

abstract class BaseResource {
  private final ServerConfig config;

  private final VersionStore<Contents, CommitMeta, Contents.Type> store;

  private final AccessChecker accessChecker;

  // Mandated by CDI 2.0
  protected BaseResource() {
    this(null, null, null);
  }

  protected BaseResource(
      ServerConfig config,
      VersionStore<Contents, CommitMeta, Contents.Type> store,
      AccessChecker accessChecker) {
    this.config = config;
    this.store = store;
    this.accessChecker = accessChecker;
  }

  WithHash<NamedRef> namedRefWithHashOrThrow(@Nullable String namedRef, @Nullable String hashOnRef)
      throws NessieNotFoundException {
    if (null == namedRef) {
      namedRef = config.getDefaultBranch();
    }
    WithHash<NamedRef> namedRefWithHash;
    try {
      WithHash<Ref> refWithHash = getStore().toRef(namedRef);
      Ref ref = refWithHash.getValue();
      if (!(ref instanceof NamedRef)) {
        throw new ReferenceNotFoundException(
            String.format("Named reference '%s' not found", namedRef));
      }
      namedRefWithHash = WithHash.of(refWithHash.getHash(), (NamedRef) refWithHash.getValue());
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(e.getMessage());
    }

    try {
      if (null == hashOnRef || store.noAncestorHash().asString().equals(hashOnRef)) {
        return namedRefWithHash;
      }

      // we need to make sure that the hash in fact exists on the named ref
      return WithHash.of(
          getStore().hashOnReference(namedRefWithHash.getValue(), Optional.of(Hash.of(hashOnRef))),
          namedRefWithHash.getValue());
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(e.getMessage());
    }
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

  protected AccessChecker getAccessChecker() {
    return accessChecker;
  }

  protected ServerAccessContext createAccessContext() {
    return ServerAccessContext.of(UUID.randomUUID().toString(), getPrincipal());
  }
}
