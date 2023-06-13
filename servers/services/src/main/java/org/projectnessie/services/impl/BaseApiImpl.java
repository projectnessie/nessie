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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN;
import static org.projectnessie.services.cel.CELUtil.CONTAINER;
import static org.projectnessie.services.cel.CELUtil.CONTENT_KEY_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.CONTENT_KEY_TYPES;
import static org.projectnessie.services.cel.CELUtil.SCRIPT_HOST;
import static org.projectnessie.services.cel.CELUtil.VAR_KEY;
import static org.projectnessie.services.cel.CELUtil.forCel;
import static org.projectnessie.versioned.RelativeCommitSpec.parseRelativeSpecs;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import javax.annotation.Nullable;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
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
import org.projectnessie.versioned.RelativeCommitSpec;
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

    NamedRef ref;
    Hash hash = null;
    Hash knownValid = null;
    if (DetachedRef.REF_NAME.equals(namedRef)) {
      ref = DetachedRef.INSTANCE;
      Objects.requireNonNull(
          hashOnRef, String.format("hashOnRef must not be null for '%s'", DetachedRef.REF_NAME));
    } else {
      try {
        ReferenceInfo<CommitMeta> refInfo =
            getStore().getNamedRef(namedRef, GetNamedRefsParams.DEFAULT);
        ref = refInfo.getNamedRef();
        knownValid = hash = refInfo.getHash();

        // Shortcut: use HEAD, if hashOnRef is null or empty
        if (hashOnRef == null || hashOnRef.isEmpty()) {
          return WithHash.of(refInfo.getHash(), ref);
        }
      } catch (ReferenceNotFoundException e) {
        throw new NessieReferenceNotFoundException(e.getMessage(), e);
      }
    }

    try {
      Matcher hashAndRelatives = HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN.matcher(hashOnRef);
      checkArgument(hashAndRelatives.find(), HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE);

      String s = hashAndRelatives.group(1);
      if (s != null) {
        hash = Hash.of(s);
      }

      Objects.requireNonNull(
          hash, // can only be null for DETACHED w/ hashOnRef w/o a starting commit ID
          String.format(
              "hashOnRef must contain a valid starting commit ID for '%s'", DetachedRef.REF_NAME));

      List<RelativeCommitSpec> relativeSpecs = parseRelativeSpecs(hashAndRelatives.group(2));

      if (store.noAncestorHash().equals(hash)) {
        // hashOnRef might point to "no ancestor hash", but the actual HEAD of the reference is
        // not necessarily the same, so construct a new instance to return.
        return WithHash.of(store.noAncestorHash(), ref);
      }

      // If the hash provided by the caller is not the current reference HEAD, verify that the hash
      // is a valid parent.
      if (!hash.equals(knownValid) || !relativeSpecs.isEmpty()) {
        hash = getStore().hashOnReference(ref, Optional.of(hash), relativeSpecs);
      }

      return WithHash.of(hash, ref);
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
    Principal principal = getPrincipal();
    String committer = principal == null ? "" : principal.getName();
    return new CommitMetaUpdater(committer, Instant.now(), commitMeta, squashMessage);
  }
}
