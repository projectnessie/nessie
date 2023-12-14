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

import jakarta.annotation.Nonnull;
import java.util.Objects;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.WithHash;

public final class RefUtil {
  private RefUtil() {}

  public static NamedRef toNamedRef(@Nonnull Reference reference) {
    Objects.requireNonNull(reference, "reference must not be null");
    if (reference instanceof Branch) {
      return BranchName.of(reference.getName());
    }
    if (reference instanceof Tag) {
      return TagName.of(reference.getName());
    }
    if (reference instanceof Detached) {
      return DetachedRef.INSTANCE;
    }
    throw new IllegalArgumentException(String.format("Unsupported reference '%s'", reference));
  }

  public static NamedRef toNamedRef(
      @Nonnull ReferenceType referenceType, @Nonnull String referenceName) {
    Objects.requireNonNull(referenceType, "referenceType must not be null");
    switch (referenceType) {
      case BRANCH:
        return BranchName.of(referenceName);
      case TAG:
        return TagName.of(referenceName);
      default:
        throw new IllegalArgumentException(
            String.format("Invalid reference type '%s'", referenceType));
    }
  }

  public static Reference toReference(@Nonnull NamedRef namedRef, Hash hash) {
    Objects.requireNonNull(namedRef, "namedRef must not be null");
    return toReference(namedRef, hash != null ? hash.asString() : null);
  }

  public static Reference toReference(@Nonnull NamedRef namedRef, String hash) {
    Objects.requireNonNull(namedRef, "namedRef must not be null");
    if (namedRef instanceof BranchName) {
      return Branch.of(namedRef.getName(), hash);
    }
    if (namedRef instanceof TagName) {
      return Tag.of(namedRef.getName(), hash);
    }
    if (namedRef instanceof DetachedRef) {
      return Detached.of(
          Objects.requireNonNull(hash, "hash must not be null for detached references"));
    }
    throw new IllegalArgumentException(String.format("Unsupported named reference '%s'", namedRef));
  }

  public static Reference toReference(@Nonnull WithHash<NamedRef> hashWitRef) {
    return toReference(hashWitRef.getValue(), hashWitRef.getHash());
  }

  public static ReferenceType referenceType(@Nonnull NamedRef namedRef) {
    if (namedRef instanceof BranchName) {
      return ReferenceType.BRANCH;
    }
    if (namedRef instanceof TagName) {
      return ReferenceType.TAG;
    }
    throw new IllegalArgumentException(String.format("Not a branch or tag: " + namedRef));
  }
}
