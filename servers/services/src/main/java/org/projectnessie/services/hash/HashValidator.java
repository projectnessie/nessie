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

import java.util.Objects;
import javax.annotation.Nullable;

@FunctionalInterface
public interface HashValidator {

  /** No-op, for any parseable hash. */
  HashValidator ANY_HASH = (name, parsed) -> {};

  /** Validates that a hash has been provided (absolute or relative). */
  HashValidator REQUIRED_HASH =
      (name, parsed) -> Objects.requireNonNull(parsed, String.format("%s must be provided.", name));

  /**
   * Validates that, if a hash was provided, it is unambiguous. A hash is unambiguous if it starts
   * with an absolute part, because it will always resolve to the same hash, even if it also has
   * relative parts.
   */
  HashValidator UNAMBIGUOUS_HASH =
      (name, parsed) ->
          checkArgument(
              parsed == null || parsed.getAbsolutePart().isPresent(),
              String.format("%s must contain a starting commit ID.", name));

  /**
   * Validates that a hash has been provided, and that it is unambiguous (that is, it starts with an
   * absolute part).
   */
  HashValidator REQUIRED_UNAMBIGUOUS_HASH = REQUIRED_HASH.and(UNAMBIGUOUS_HASH);

  /**
   * Validates the provided hash.
   *
   * @param name the name of the hash parameter, for error messages
   * @param parsed the parsed hash, or {@code null} if no hash was provided
   */
  void validate(String name, @Nullable @jakarta.annotation.Nullable ParsedHash parsed);

  /**
   * Returns a new {@link HashValidator} that validates against both {@code this} and {@code other}.
   */
  default HashValidator and(HashValidator other) {
    return (name, parsed) -> {
      validate(name, parsed);
      other.validate(name, parsed);
    };
  }
}
