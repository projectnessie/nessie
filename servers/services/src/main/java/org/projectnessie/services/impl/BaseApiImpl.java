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
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
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

abstract class BaseApiImpl {
  private final ServerConfig config;
  private final VersionStore<Contents, CommitMeta, Contents.Type> store;
  private final AccessChecker accessChecker;
  private final Principal principal;

  protected BaseApiImpl(
      ServerConfig config,
      VersionStore<Contents, CommitMeta, Contents.Type> store,
      AccessChecker accessChecker,
      Principal principal) {
    this.config = config;
    this.store = store;
    this.accessChecker = accessChecker;
    this.principal = principal;
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
      namedRefWithHash = WithHash.of(refWithHash.getHash(), (NamedRef) ref);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    try {
      if (null == hashOnRef || store.noAncestorHash().asString().equals(hashOnRef)) {
        return namedRefWithHash;
      }

      // the version store already gave us the hash on namedRef, so we can skip checking whether the
      // hash actually exists on the named reference and return early here
      if (namedRefWithHash.getHash().asString().equals(hashOnRef)) {
        return namedRefWithHash;
      }

      // we need to make sure that the hash in fact exists on the named ref
      return WithHash.of(
          getStore().hashOnReference(namedRefWithHash.getValue(), Optional.of(Hash.of(hashOnRef))),
          namedRefWithHash.getValue());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  protected ServerConfig getConfig() {
    return config;
  }

  protected VersionStore<Contents, CommitMeta, Contents.Type> getStore() {
    return store;
  }

  protected Principal getPrincipal() {
    return principal;
  }

  protected AccessChecker getAccessChecker() {
    return accessChecker;
  }

  protected ServerAccessContext createAccessContext() {
    return ServerAccessContext.of(UUID.randomUUID().toString(), getPrincipal());
  }
}
