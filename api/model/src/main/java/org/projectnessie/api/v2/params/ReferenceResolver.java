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
import static org.projectnessie.model.Validation.REF_NAME_PATH_PATTERN;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Validation;

public final class ReferenceResolver {

  private static final String DEFAULT_REF_IN_PATH = "-";

  private ReferenceResolver() {}

  public static ParsedReference resolveReferencePathElement(
      @Nonnull @jakarta.annotation.Nonnull String refPathString,
      @Nullable @jakarta.annotation.Nullable Reference.ReferenceType namedRefType,
      @Nonnull @jakarta.annotation.Nonnull Supplier<String> defaultBranchSupplier) {
    Matcher refNameMatcher = REF_NAME_PATH_PATTERN.matcher(refPathString);
    if (!refNameMatcher.find()) {
      throw new IllegalArgumentException(Validation.REF_NAME_MESSAGE);
    }
    String name = refNameMatcher.group(1);
    if (DEFAULT_REF_IN_PATH.equals(name)) {
      name = defaultBranchSupplier.get();
    }
    String hash = refNameMatcher.group(2);
    String relativeSpec = refNameMatcher.group(3);
    if (hash != null && relativeSpec != null) {
      hash += relativeSpec;
    } else if (relativeSpec != null) {
      hash = relativeSpec;
    }
    return parsedReference(name, hash, name != null ? namedRefType : null);
  }
}
