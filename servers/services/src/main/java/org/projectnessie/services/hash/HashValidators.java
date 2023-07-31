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
import java.util.function.BiConsumer;

public final class HashValidators {

  private HashValidators() {}

  /** Validates any parseable hash. */
  public static final BiConsumer<String, ParsedHash> ANY_HASH = (name, parsed) -> {};

  /** Validates that a hash has been provided (absolute or relative). */
  public static final BiConsumer<String, ParsedHash> REQUIRED_HASH =
      (name, parsed) -> Objects.requireNonNull(parsed, String.format("%s must be provided.", name));

  /**
   * Validates that, if a hash was provided, it is unambiguous. A hash is unambiguous if it has an
   * absolute part, because it will always resolve to the same hash, even if it also has relative
   * parts.
   */
  public static final BiConsumer<String, ParsedHash> UNAMBIGUOUS_HASH =
      (name, parsed) ->
          checkArgument(
              parsed == null || parsed.getAbsolutePart().isPresent(),
              String.format("%s must contain a starting commit ID.", name));

  /**
   * Validates that a hash has been provided, and that it is unambiguous (that is, it contains an
   * absolute part).
   */
  public static final BiConsumer<String, ParsedHash> REQUIRED_UNAMBIGUOUS_HASH =
      REQUIRED_HASH.andThen(UNAMBIGUOUS_HASH);
}
