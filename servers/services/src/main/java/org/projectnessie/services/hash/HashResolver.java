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

import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

public final class HashResolver {

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
  public ResolvedHash resolveToHead(@Nullable String namedRef) throws ReferenceNotFoundException {
    checkArgument(
        namedRef == null || !namedRef.equals(DetachedRef.REF_NAME),
        "Cannot resolve DETACHED to HEAD");
    return resolveHashOnRef(namedRef, null);
  }

  /**
   * Resolves the given {@code namedRef} to {@code hashOnRef} if present, otherwise to its current
   * HEAD, if available. Uses the {@link HashValidator#DEFAULT} instance.
   *
   * <p>Throws if the reference does not exist, the hash is invalid, or is not present on the
   * reference.
   *
   * <p>If {@code namedRef} is {@link DetachedRef}, then a non-null {@code hashOnRef} is required.
   *
   * <p>If {@code namedRef} is null, the default branch will be used.
   */
  public ResolvedHash resolveHashOnRef(@Nullable String namedRef, @Nullable String hashOnRef)
      throws ReferenceNotFoundException {
    return resolveHashOnRef(namedRef, hashOnRef, HashValidator.DEFAULT);
  }

  /**
   * Same as {@link #resolveHashOnRef(String, String)} but allows to pass a custom {@link
   * HashValidator}.
   */
  public ResolvedHash resolveHashOnRef(
      @Nullable String namedRef, @Nullable String hashOnRef, HashValidator validator)
      throws ReferenceNotFoundException {
    if (null == namedRef) {
      namedRef = config.getDefaultBranch();
    }
    NamedRef ref;
    Hash currentHead = null;
    if (DetachedRef.REF_NAME.equals(namedRef)) {
      ref = DetachedRef.INSTANCE;
    } else {
      ReferenceInfo<CommitMeta> refInfo = store.getNamedRef(namedRef, GetNamedRefsParams.DEFAULT);
      ref = refInfo.getNamedRef();
      currentHead = refInfo.getHash();
    }
    return resolveHashOnRef(ref, currentHead, hashOnRef, validator);
  }

  /**
   * Resolves the given {@code hashOnRef} against another previously computed {@link ResolvedHash}
   * pointing to a branch at its HEAD.
   *
   * <p>This is useful to compute more hashes against a first resolved hash, e.g. when
   * transplanting.
   *
   * <p>See {@link #resolveHashOnRef(String, String)} for important caveats.
   */
  public ResolvedHash resolveHashOnRef(
      ResolvedHash head, @Nullable String hashOnRef, HashValidator validator)
      throws ReferenceNotFoundException {
    return resolveHashOnRef(head.getNamedRef(), head.getHead().orElse(null), hashOnRef, validator);
  }

  /**
   * Resolves the given {@code namedRef} to {@code hashOnRef} if present, otherwise to {@code
   * currentHead}, if available.
   *
   * <p>Either {@code currentHead} or {@code hashOnRef} must be non-null. It's the caller's
   * responsibility to validate that any user-provided input meets this requirement.
   *
   * <p>See {@link #resolveHashOnRef(String, String)} for important caveats.
   */
  public ResolvedHash resolveHashOnRef(
      NamedRef ref, @Nullable Hash currentHead, @Nullable String hashOnRef, HashValidator validator)
      throws ReferenceNotFoundException {
    checkState(currentHead != null || hashOnRef != null);
    Optional<ParsedHash> parsed = ParsedHash.parse(hashOnRef, store.noAncestorHash());
    validator.validate(ref, parsed.orElse(null));
    Hash resolved = parsed.flatMap(ParsedHash::getAbsolutePart).orElse(currentHead);
    checkState(resolved != null);
    List<RelativeCommitSpec> relativeParts =
        parsed.map(ParsedHash::getRelativeParts).orElse(Collections.emptyList());
    if (!relativeParts.isEmpty()) {
      // Resolve the hash against DETACHED because we are only interested in
      // resolving the hash, not checking if it is on the branch. This will
      // be done later on.
      resolved = store.hashOnReference(DetachedRef.INSTANCE, Optional.of(resolved), relativeParts);
    }
    return ResolvedHash.of(ref, Optional.ofNullable(currentHead), resolved);
  }
}
