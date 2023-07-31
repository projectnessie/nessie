/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.hash;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.services.hash.HashValidators.ANY_HASH;
import static org.projectnessie.services.hash.HashValidators.REQUIRED_UNAMBIGUOUS_HASH;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RelativeCommitSpec;
import org.projectnessie.versioned.VersionStore;

public class HashResolver {

  private final ServerConfig config;
  private final VersionStore store;

  public HashResolver(ServerConfig config, VersionStore store) {
    this.config = config;
    this.store = store;
  }

  /**
   * Resolves the given {@code namedRef} to its current HEAD. Throws if the reference does not
   * exist, or if it's {@link DetachedRef}.
   *
   * <p>If {@code namedRef} is null, the default branch will be used.
   */
  public ResolvedHash resolveToHead(@Nullable @jakarta.annotation.Nullable String namedRef)
      throws NessieReferenceNotFoundException {
    checkArgument(
        namedRef == null || !namedRef.equals(DetachedRef.REF_NAME),
        "Cannot resolve DETACHED to HEAD");
    return resolveHashOnRef(namedRef, null, "", ANY_HASH);
  }

  /**
   * Resolves the given {@code namedRef} to {@code hashOnRef} if present, otherwise to its current
   * HEAD, if available.
   *
   * <p>Throws if the reference does not exist, the hash is invalid, or is not present on the
   * reference.
   *
   * <p>If {@code namedRef} is {@link DetachedRef}, then a non-null {@code hashOnRef} is required.
   *
   * <p>If {@code namedRef} is null, the default branch will be used.
   *
   * <p>The parameter {name} is used for error messages.
   *
   * <p>A hash validator must be provided to perform extra validations on the parsed hashed. If no
   * extra validation is required, {@link HashValidators#ANY_HASH} can be used.
   */
  public ResolvedHash resolveHashOnRef(
      @Nullable @jakarta.annotation.Nullable String namedRef,
      @Nullable @jakarta.annotation.Nullable String hashOnRef,
      String name,
      BiConsumer<String, ParsedHash> validator)
      throws NessieReferenceNotFoundException {
    if (null == namedRef) {
      namedRef = config.getDefaultBranch();
    }
    try {
      NamedRef ref;
      Hash currentHead = null;
      if (DetachedRef.REF_NAME.equals(namedRef)) {
        ref = DetachedRef.INSTANCE;
      } else {
        ReferenceInfo<CommitMeta> refInfo = store.getNamedRef(namedRef, GetNamedRefsParams.DEFAULT);
        ref = refInfo.getNamedRef();
        currentHead = refInfo.getHash();
        // Shortcut: use HEAD, if hashOnRef is null or empty
        if (hashOnRef == null || hashOnRef.isEmpty()) {
          return ResolvedHash.of(ref, Optional.empty(), Optional.of(currentHead));
        }
      }
      return resolveHashOnRef(ref, currentHead, hashOnRef, name, validator);
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  /**
   * Resolves the given {@code hashOnRef} against another previously computed {@link ResolvedHash}
   * pointing to a branch at its HEAD.
   *
   * <p>This is useful to compute more hashes against a first resolved hash, e.g. when
   * transplanting.
   *
   * <p>See {@link #resolveHashOnRef(String, String, String, BiConsumer)} for important caveats.
   */
  public ResolvedHash resolveHashOnRef(
      ResolvedHash head,
      @Nullable @jakarta.annotation.Nullable String hashOnRef,
      String name,
      BiConsumer<String, ParsedHash> validator)
      throws ReferenceNotFoundException {
    return resolveHashOnRef(
        head.getNamedRef(), head.getHead().orElse(null), hashOnRef, name, validator);
  }

  /**
   * Resolves the given {@code namedRef} to {@code hashOnRef} if present, otherwise to {@code
   * currentHead}, if available.
   *
   * <p>Either {@code currentHead} or {@code hashOnRef} must be non-null. It's the caller's
   * responsibility to validate that any user-provided input meets this requirement.
   *
   * <p>See {@link #resolveHashOnRef(String, String, String, BiConsumer)} for important caveats.
   */
  public ResolvedHash resolveHashOnRef(
      NamedRef ref,
      @Nullable @jakarta.annotation.Nullable Hash currentHead,
      @Nullable @jakarta.annotation.Nullable String hashOnRef,
      String name,
      BiConsumer<String, ParsedHash> validator)
      throws ReferenceNotFoundException {
    checkState(currentHead != null || hashOnRef != null);
    Optional<ParsedHash> parsed = ParsedHash.parse(hashOnRef, store.noAncestorHash());
    if (ref == DetachedRef.INSTANCE) {
      validator = validator.andThen(REQUIRED_UNAMBIGUOUS_HASH);
    }
    validator.accept(name, parsed.orElse(null));
    Hash startOrHead = parsed.flatMap(ParsedHash::getAbsolutePart).orElse(currentHead);
    checkState(startOrHead != null);
    List<RelativeCommitSpec> relativeParts =
        parsed.map(ParsedHash::getRelativeParts).orElse(Collections.emptyList());
    Optional<Hash> resolved = Optional.of(startOrHead);
    if (!Objects.equals(startOrHead, currentHead) || !relativeParts.isEmpty()) {
      resolved = Optional.ofNullable(store.hashOnReference(ref, resolved, relativeParts));
    }
    return ResolvedHash.of(ref, resolved, Optional.ofNullable(currentHead));
  }
}
