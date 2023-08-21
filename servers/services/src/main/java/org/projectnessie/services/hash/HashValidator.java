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
import javax.annotation.Nullable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

public final class HashValidator {

  public static final HashValidator DEFAULT = new HashValidator();

  private final String refName;
  private final String hashName;

  private HashValidation validation = (namedRef, parsed) -> {};

  public HashValidator() {
    this("Hash");
  }

  public HashValidator(String hashName) {
    this("Reference", hashName);
  }

  public HashValidator(String refName, String hashName) {
    this.refName = refName;
    this.hashName = hashName;
  }

  /**
   * Validates the provided ref and hash.
   *
   * @param namedRef the namedRef, required.
   * @param parsed the parsed hash, or {@code null} if no hash was provided.
   */
  public void validate(
      NamedRef namedRef, @Nullable @jakarta.annotation.Nullable ParsedHash parsed) {
    validation.validate(namedRef, parsed);
  }

  /** Validates that a named ref is a branch. */
  @CanIgnoreReturnValue
  public HashValidator refMustBeBranch() {
    validation = validation.and(HashValidation.refMustBeBranch(refName));
    return this;
  }

  /** Validates that a named ref is a branch or a tag. */
  @CanIgnoreReturnValue
  public HashValidator refMustBeBranchOrTag() {
    validation = validation.and(HashValidation.refMustBeBranchOrTag(refName));
    return this;
  }

  /** Validates that a hash has been provided (absolute or relative). */
  @CanIgnoreReturnValue
  public HashValidator hashMustBePresent() {
    validation = validation.and(HashValidation.hashMustBePresent(hashName));
    return this;
  }

  /**
   * Validates that, if a hash was provided, it is unambiguous. A hash is unambiguous if it starts
   * with an absolute part, because it will always resolve to the same hash, even if it also has
   * relative parts.
   */
  @CanIgnoreReturnValue
  public HashValidator hashMustNotBeAmbiguous() {
    validation = validation.and(HashValidation.hashMustNotBeAmbiguous(hashName));
    return this;
  }

  @FunctionalInterface
  private interface HashValidation {

    static HashValidation refMustBeBranch(String refName) {
      return (namedRef, parsed) ->
          checkArgument(namedRef instanceof BranchName, "%s must be a branch.", refName);
    }

    static HashValidation refMustBeBranchOrTag(String refName) {
      return (namedRef, parsed) ->
          checkArgument(
              namedRef instanceof BranchName || namedRef instanceof TagName,
              "%s must be a branch or a tag.",
              refName);
    }

    static HashValidation hashMustBePresent(String hashName) {
      return (namedRef, parsed) -> checkArgument(parsed != null, "%s must be provided.", hashName);
    }

    static HashValidation hashMustNotBeAmbiguous(String hashName) {
      return (namedRef, parsed) ->
          checkArgument(
              parsed == null || parsed.getAbsolutePart().isPresent(),
              "%s must contain a starting commit ID.",
              hashName);
    }

    /**
     * Validates the provided ref and hash.
     *
     * @param namedRef the namedRef, required.
     * @param parsed the parsed hash, or {@code null} if no hash was provided
     */
    void validate(NamedRef namedRef, @Nullable @jakarta.annotation.Nullable ParsedHash parsed);

    /**
     * Returns a new {@link HashValidator} that validates against both {@code this} and {@code
     * other}.
     */
    default HashValidation and(HashValidation other) {
      return (namedRef, parsed) -> {
        validate(namedRef, parsed);
        other.validate(namedRef, parsed);
      };
    }
  }
}
