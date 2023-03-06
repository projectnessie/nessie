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
package org.projectnessie.api.v2.params;

import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.model.Reference;

public final class ReferenceResolver {

  public static final String DEFAULT_REF_IN_PATH = "-";
  public static final char REF_HASH_SEPARATOR = '@';

  private ReferenceResolver() {}

  public static ParsedReference resolveReferencePathElement(
      @Nonnull @jakarta.annotation.Nonnull String value,
      @Nullable @jakarta.annotation.Nullable Reference.ReferenceType namedRefType) {
    String name = null;
    String hash = null;
    int hashIdx = value.indexOf(REF_HASH_SEPARATOR);

    if (hashIdx > 0) {
      name = value.substring(0, hashIdx);
    }

    if (hashIdx < 0) {
      name = value;
    }

    if (hashIdx >= 0) {
      hash = value.substring(hashIdx + 1);
      if (hash.isEmpty()) {
        hash = null;
      }
    }

    if (name != null) {
      return parsedReference(name, hash, namedRefType);
    }
    // detached
    return parsedReference(null, hash, null);
  }

  public static ParsedReference resolveReferencePathElementWithDefaultBranch(
      @Nonnull @jakarta.annotation.Nonnull String refPathString,
      @Nonnull @jakarta.annotation.Nonnull Supplier<String> defaultBranchSupplier) {
    if (!DEFAULT_REF_IN_PATH.equals(refPathString)) {
      return resolveReferencePathElement(refPathString, null);
    }

    return parsedReference(defaultBranchSupplier.get(), null, BRANCH);
  }
}
