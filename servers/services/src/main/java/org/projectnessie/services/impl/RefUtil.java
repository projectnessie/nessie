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

import java.util.Objects;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

public final class RefUtil {
  private RefUtil() {}

  public static NamedRef toNamedRef(Reference reference) {
    Objects.requireNonNull(reference, "reference must not be null");
    if (reference instanceof Branch) {
      return BranchName.of(reference.getName());
    }
    if (reference instanceof Tag) {
      return TagName.of(reference.getName());
    }
    throw new IllegalArgumentException(String.format("Unsupported reference '%s'", reference));
  }

  public static NamedRef toNamedRef(Reference.ReferenceType referenceType, String referenceName) {
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

  public static Reference toReference(NamedRef namedRef, Hash hash) {
    Objects.requireNonNull(namedRef, "namedRef must not be null");
    return toReference(namedRef, hash != null ? hash.asString() : null);
  }

  public static Reference toReference(NamedRef namedRef, String hash) {
    Objects.requireNonNull(namedRef, "namedRef must not be null");
    if (namedRef instanceof BranchName) {
      return Branch.of(namedRef.getName(), hash);
    }
    if (namedRef instanceof TagName) {
      return Tag.of(namedRef.getName(), hash);
    }
    throw new IllegalArgumentException(String.format("Unsupported named reference '%s'", namedRef));
  }
}
