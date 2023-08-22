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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

public final class HashValidator {

  public static final HashValidator DEFAULT = new HashValidator();

  private final String refDescription;
  private final String hashDescription;

  private BiConsumer<NamedRef, ParsedHash> validator = (namedRef, parsed) -> {};

  public HashValidator() {
    this("Hash");
  }

  public HashValidator(String hashDescription) {
    this("Reference", hashDescription);
  }

  public HashValidator(String refDescription, String hashDescription) {
    this.refDescription = refDescription;
    this.hashDescription = hashDescription;
  }

  /**
   * Validates the provided ref and hash.
   *
   * @param namedRef the namedRef, required.
   * @param parsed the parsed hash, or {@code null} if no hash was provided.
   */
  public void validate(
      NamedRef namedRef, @Nullable @jakarta.annotation.Nullable ParsedHash parsed) {
    validator.accept(namedRef, parsed);
  }

  /** Validates that a named ref is a branch. */
  @CanIgnoreReturnValue
  public HashValidator refMustBeBranch() {
    validator =
        validator.andThen(
            (namedRef, parsed) ->
                checkArgument(
                    namedRef instanceof BranchName, "%s must be a branch.", refDescription));
    return this;
  }

  /** Validates that a named ref is a branch or a tag. */
  @CanIgnoreReturnValue
  public HashValidator refMustBeBranchOrTag() {
    validator =
        validator.andThen(
            (namedRef, parsed) ->
                checkArgument(
                    namedRef instanceof BranchName || namedRef instanceof TagName,
                    "%s must be a branch or a tag.",
                    refDescription));
    return this;
  }

  /** Validates that a hash has been provided (absolute or relative). */
  @CanIgnoreReturnValue
  public HashValidator hashMustBePresent() {
    validator =
        validator.andThen(
            (namedRef, parsed) ->
                checkArgument(parsed != null, "%s must be provided.", hashDescription));
    return this;
  }

  /**
   * Validates that, if a hash was provided, it is unambiguous. A hash is unambiguous if it starts
   * with an absolute part, because it will always resolve to the same hash, even if it also has
   * relative parts.
   */
  @CanIgnoreReturnValue
  public HashValidator hashMustNotBeAmbiguous() {
    validator =
        validator.andThen(
            (namedRef, parsed) ->
                checkArgument(
                    parsed == null || parsed.getAbsolutePart().isPresent(),
                    "%s must contain a starting commit ID.",
                    hashDescription));
    return this;
  }
}
