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
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.ServerAccessContext;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

abstract class BaseApiImpl {
  private final ServerConfig config;
  private final VersionStore<Content, CommitMeta, Content.Type> store;
  private final Authorizer authorizer;
  private final Principal principal;

  protected BaseApiImpl(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      Authorizer authorizer,
      Principal principal) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
    this.principal = principal;
  }

  WithHash<NamedRef> namedRefWithHashOrThrow(@Nullable String namedRef, @Nullable String hashOnRef)
      throws NessieNotFoundException {
    if (null == namedRef) {
      namedRef = config.getDefaultBranch();
    }

    if (DetachedRef.REF_NAME.equals(namedRef)) {
      Objects.requireNonNull(
          hashOnRef, String.format("hashOnRef must not be null for '%s'", DetachedRef.REF_NAME));
      return WithHash.of(Hash.of(hashOnRef), DetachedRef.INSTANCE);
    }

    WithHash<NamedRef> namedRefWithHash;
    try {
      ReferenceInfo<CommitMeta> ref = getStore().getNamedRef(namedRef, GetNamedRefsParams.DEFAULT);
      namedRefWithHash = WithHash.of(ref.getHash(), ref.getNamedRef());
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }

    try {
      if (null == hashOnRef) {
        return namedRefWithHash;
      }
      if (store.noAncestorHash().asString().equals(hashOnRef)) {
        // hashOnRef might point to "no ancestor hash", but the actual HEAD of the reference is not
        // necessarily the same, so construct a new instance to return.
        return WithHash.of(store.noAncestorHash(), namedRefWithHash.getValue());
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

  protected VersionStore<Content, CommitMeta, Content.Type> getStore() {
    return store;
  }

  protected Principal getPrincipal() {
    return principal;
  }

  protected Authorizer getAuthorizer() {
    return authorizer;
  }

  protected BatchAccessChecker startAccessCheck() {
    return getAuthorizer().startAccessCheck(createAccessContext());
  }

  protected ServerAccessContext createAccessContext() {
    return ServerAccessContext.of(UUID.randomUUID().toString(), getPrincipal());
  }

  protected MetadataRewriter<CommitMeta> commitMetaUpdate() {
    return new MetadataRewriter<CommitMeta>() {
      // Used for setting contextual commit properties during new and merge/transplant commits.
      // WARNING: ONLY SET PROPERTIES, WHICH APPLY COMMONLY TO ALL COMMIT TYPES.
      private final Principal principal = getPrincipal();
      private final String committer = principal == null ? "" : principal.getName();
      private final Instant now = Instant.now();

      @Override
      public CommitMeta rewriteSingle(CommitMeta metadata) {
        return metadata.toBuilder()
            .committer(committer)
            .commitTime(now)
            .author(metadata.getAuthor() == null ? committer : metadata.getAuthor())
            .authorTime(metadata.getAuthorTime() == null ? now : metadata.getAuthorTime())
            .build();
      }

      @Override
      public CommitMeta squash(List<CommitMeta> metadata) {
        if (metadata.size() == 1) {
          return rewriteSingle(metadata.get(0));
        }

        ImmutableCommitMeta.Builder newMeta =
            CommitMeta.builder()
                .committer(committer)
                .commitTime(now)
                .author(committer)
                .authorTime(now);
        StringBuilder newMessage = new StringBuilder();
        Map<String, String> newProperties = new HashMap<>();
        for (CommitMeta commitMeta : metadata) {
          newProperties.putAll(commitMeta.getProperties());
          if (newMessage.length() > 0) {
            newMessage.append("\n---------------------------------------------\n");
          }
          newMessage.append(commitMeta.getMessage());
        }
        return newMeta.putAllProperties(newProperties).message(newMessage.toString()).build();
      }
    };
  }
}
