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

import static java.util.Collections.singletonList;
import static org.projectnessie.services.cel.CELUtil.CONTAINER;
import static org.projectnessie.services.cel.CELUtil.CONTENT_KEY_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.CONTENT_KEY_TYPES;
import static org.projectnessie.services.cel.CELUtil.SCRIPT_HOST;
import static org.projectnessie.services.cel.CELUtil.VAR_KEY;
import static org.projectnessie.services.cel.CELUtil.forCel;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
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

public abstract class BaseApiImpl {
  private final ServerConfig config;
  private final VersionStore store;
  private final Authorizer authorizer;
  private final Supplier<Principal> principal;

  protected static final int ACCESS_CHECK_BATCH_SIZE = 10;

  protected BaseApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
    this.principal = principal;
  }

  /**
   * Produces the filter predicate for content-key filtering.
   *
   * @param filter The content-key filter expression, if not empty
   */
  static Predicate<ContentKey> filterOnContentKey(String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return x -> true;
    }

    final Script script;
    try {
      script =
          SCRIPT_HOST
              .buildScript(filter)
              .withContainer(CONTAINER)
              .withDeclarations(CONTENT_KEY_DECLARATIONS)
              .withTypes(CONTENT_KEY_TYPES)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return key -> {
      try {
        return script.execute(Boolean.class, ImmutableMap.of(VAR_KEY, forCel(key)));
      } catch (ScriptException e) {
        throw new RuntimeException(e);
      }
    };
  }

  WithHash<NamedRef> namedRefWithHashOrThrow(
      @Nullable @jakarta.annotation.Nullable String namedRef,
      @Nullable @jakarta.annotation.Nullable String hashOnRef)
      throws NessieReferenceNotFoundException {
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

  protected VersionStore getStore() {
    return store;
  }

  protected Principal getPrincipal() {
    return principal.get();
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

  protected MetadataRewriter<CommitMeta> commitMetaUpdate(
      @Nullable @jakarta.annotation.Nullable CommitMeta commitMeta,
      IntFunction<String> squashMessage) {
    return new MetadataRewriter<CommitMeta>() {
      // Used for setting contextual commit properties during new and merge/transplant commits.
      // WARNING: ONLY SET PROPERTIES, WHICH APPLY COMMONLY TO ALL COMMIT TYPES.
      private final Principal principal = getPrincipal();
      private final String committer = principal == null ? "" : principal.getName();
      private final Instant now = Instant.now();

      private CommitMeta buildCommitMeta(
          ImmutableCommitMeta.Builder metadata, Supplier<String> defaultMessage) {

        ImmutableCommitMeta pre = metadata.message("").build();

        if (commitMeta != null && !commitMeta.getAllAuthors().isEmpty()) {
          metadata.allAuthors(commitMeta.getAllAuthors());
        } else if (pre.getAllAuthors().isEmpty()) {
          metadata.allAuthors(singletonList(committer));
        }

        if (commitMeta != null && !commitMeta.getAllSignedOffBy().isEmpty()) {
          metadata.allSignedOffBy(commitMeta.getAllSignedOffBy());
        }

        if (commitMeta != null && commitMeta.getAuthorTime() != null) {
          metadata.authorTime(commitMeta.getAuthorTime());
        } else if (pre.getAuthorTime() == null) {
          metadata.authorTime(now);
        }

        if (commitMeta != null && !commitMeta.getAllProperties().isEmpty()) {
          metadata.allProperties(commitMeta.getAllProperties());
        }

        if (commitMeta != null && !commitMeta.getMessage().isEmpty()) {
          metadata.message(commitMeta.getMessage());
        } else {
          metadata.message(defaultMessage.get());
        }

        return metadata.committer(committer).commitTime(now).build();
      }

      @Override
      public CommitMeta rewriteSingle(CommitMeta metadata) {
        return buildCommitMeta(CommitMeta.builder().from(metadata), metadata::getMessage);
      }

      @Override
      public CommitMeta squash(List<CommitMeta> metadata) {
        Optional<String> msg = Optional.ofNullable(squashMessage.apply(metadata.size()));

        if (metadata.size() == 1 && !msg.isPresent()) {
          return rewriteSingle(metadata.get(0));
        }

        Map<String, String> newProperties = new HashMap<>();
        for (CommitMeta commitMeta : metadata) {
          newProperties.putAll(commitMeta.getProperties());
        }

        return buildCommitMeta(
            CommitMeta.builder().properties(newProperties),
            () ->
                msg.orElseGet(
                    () -> {
                      StringBuilder newMessage = new StringBuilder();
                      for (CommitMeta commitMeta : metadata) {
                        if (newMessage.length() > 0) {
                          newMessage.append("\n---------------------------------------------\n");
                        }
                        newMessage.append(commitMeta.getMessage());
                      }
                      return newMessage.toString();
                    }));
      }
    };
  }
}
