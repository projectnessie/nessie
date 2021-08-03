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
import javax.ws.rs.core.SecurityContext;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Validation;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.authz.ServerAccessContext;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

abstract class BaseResource {
  private final ServerConfig config;

  private final MultiTenant multiTenant;

  private final VersionStore<Contents, CommitMeta, Contents.Type> store;

  private final AccessChecker accessChecker;

  // Mandated by CDI 2.0
  protected BaseResource() {
    this(null, null, null, null);
  }

  protected BaseResource(
      ServerConfig config,
      MultiTenant multiTenant,
      VersionStore<Contents, CommitMeta, Contents.Type> store,
      AccessChecker accessChecker) {
    this.config = config;
    this.multiTenant = multiTenant;
    this.store = store;
    this.accessChecker = accessChecker;
  }

  NamedRef namedRefOrThrow(String ref) throws NessieNotFoundException {
    return namedRefWithHashOrThrow(ref).getValue();
  }

  Optional<NamedRef> optionalNamedRefOrThrow(String ref) throws NessieNotFoundException {
    if (ref == null || ref.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(namedRefWithHashOrThrow(ref).getValue());
  }

  WithHash<NamedRef> namedRefWithHashOrThrow(String ref) throws NessieNotFoundException {
    try {
      if (null != ref) {
        ref = Validation.validateReferenceName(ref);
      }
      return store.toRef(Optional.ofNullable(ref).orElse(config.getDefaultBranch()));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(String.format("Ref for %s not found", ref));
    }
  }

  static Optional<Hash> toHash(String hash, boolean required) throws NessieConflictException {
    try {
      return toHashIA(hash, required);
    } catch (IllegalArgumentException e) {
      throw new NessieConflictException("Must provide expected hash value for operation.");
    }
  }

  static Optional<Hash> toHashIA(String hash, boolean required) {
    if (hash == null || hash.isEmpty()) {
      if (required) {
        throw new IllegalArgumentException("Must provide expected hash value for operation.");
      }
      return Optional.empty();
    }
    return Optional.of(Hash.of(Validation.validateHash(hash)));
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

  public MultiTenant getMultiTenant() {
    return multiTenant;
  }

  protected AccessChecker getAccessChecker() {
    return accessChecker;
  }

  protected ServerAccessContext createAccessContext() {
    return ServerAccessContext.of(UUID.randomUUID().toString(), getPrincipal());
  }
}
