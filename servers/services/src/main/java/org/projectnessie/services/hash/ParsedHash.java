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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN;
import static org.projectnessie.versioned.RelativeCommitSpec.parseRelativeSpecs;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.RelativeCommitSpec;

/**
 * A parsed hash, which can contain either an absolute hash, a relative commit spec, or both.
 *
 * <p>It is package-private, because it is only used by {@link HashResolver}.
 */
@Value.Immutable
public interface ParsedHash {

  Optional<Hash> getAbsolutePart();

  List<RelativeCommitSpec> getRelativeParts();

  static ParsedHash of(Hash absolutePart) {
    return ImmutableParsedHash.builder().absolutePart(absolutePart).build();
  }

  static ParsedHash of(RelativeCommitSpec... relativeParts) {
    return ImmutableParsedHash.builder().addRelativeParts(relativeParts).build();
  }

  static ParsedHash of(Hash absolutePart, RelativeCommitSpec... relativeParts) {
    return ImmutableParsedHash.builder()
        .absolutePart(absolutePart)
        .addRelativeParts(relativeParts)
        .build();
  }

  static Optional<ParsedHash> parse(
      @Nullable String hashOrRelativeSpec, @Nonnull Hash noAncestorHash) {
    requireNonNull(noAncestorHash, "noAncestorHash");
    if (hashOrRelativeSpec == null || hashOrRelativeSpec.isEmpty()) {
      return Optional.empty();
    }
    Matcher matcher = HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN.matcher(hashOrRelativeSpec);
    checkArgument(matcher.matches(), HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE, hashOrRelativeSpec);
    Optional<Hash> absolutePart =
        Optional.ofNullable(matcher.group(1))
            .map(Hash::of)
            .map(h -> h.equals(noAncestorHash) ? noAncestorHash : h);
    List<RelativeCommitSpec> relativePart = parseRelativeSpecs(matcher.group(2));
    return Optional.of(
        ImmutableParsedHash.builder()
            .absolutePart(absolutePart)
            .relativeParts(relativePart)
            .build());
  }
}
