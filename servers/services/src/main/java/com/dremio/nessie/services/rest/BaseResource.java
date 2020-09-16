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

import java.security.Principal;
import java.util.Optional;

import javax.inject.Inject;

import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;

abstract class BaseResource {

  @Inject
  protected Principal principal;

  @Inject
  protected VersionStore<Contents, CommitMeta> store;

  Optional<Hash> getHash(String ref) {
    try {
      WithHash<Ref> whr = store.toRef(ref);
      return Optional.of(whr.getHash());
    } catch (ReferenceNotFoundException e) {
      return Optional.empty();
    }
  }

  Hash getHashOrThrow(String ref) throws NessieNotFoundException {
    return getHash(ref).orElseThrow(() -> new NessieNotFoundException(String.format("Ref for %s not found", ref)));
  }

}
