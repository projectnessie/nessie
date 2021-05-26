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
package org.projectnessie.versioned;

import static java.lang.String.format;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Exception thrown when the hash associated with a named reference does not match with the hash
 * provided by the caller.
 */
public class ReferenceConflictException extends VersionStoreException {
  private static final long serialVersionUID = 4381980193289615523L;

  public ReferenceConflictException(String message) {
    super(message);
  }

  public ReferenceConflictException(String message, Throwable t) {
    super(message, t);
  }

  /**
   * Create a {@code ReferenceConflictException} instance with an accurate message based on the
   * provided named referenced and the compared hashes.
   *
   * @param ref the named reference
   * @param expected the hash expected to be found in the store
   * @param actual the hash found in the store for the reference
   * @return a {@code ReferenceNotFoundException} instance
   * @throws NullPointerException if {@code ref} is {@code null}.
   */
  @Nonnull
  public static ReferenceConflictException forReference(
      @Nonnull NamedRef ref, @Nonnull Optional<Hash> expected, @Nonnull Optional<Hash> actual) {
    final String expectedArgument = expected.map(Hash::asString).orElse("no reference");
    final String actualArgument = actual.map(Hash::asString).orElse("no reference");
    final String refType;
    if (ref instanceof BranchName) {
      refType = "branch";
    } else if (ref instanceof TagName) {
      refType = "tag";
    } else {
      refType = "named ref";
    }
    return new ReferenceConflictException(
        format(
            "Expected %s for %s '%s' but was %s",
            expectedArgument, refType, ref.getName(), actualArgument));
  }

  /**
   * Create a {@code ReferenceConflictException} instance with an accurate message based on the
   * provided named referenced and the compared hashes.
   *
   * @param ref the named reference
   * @param expected the hash expected to be found in the store
   * @param actual the hash found in the store for the reference
   * @return a {@code ReferenceNotFoundException} instance
   * @throws NullPointerException if {@code ref} is {@code null}.
   */
  @Nonnull
  public static ReferenceConflictException forReference(
      @Nonnull NamedRef ref,
      @Nonnull Optional<Hash> expected,
      @Nonnull Optional<Hash> actual,
      @Nonnull Throwable t) {
    final String expectedArgument = expected.map(Hash::asString).orElse("no reference");
    final String actualArgument = actual.map(Hash::asString).orElse("no reference");
    final String refType;
    if (ref instanceof BranchName) {
      refType = "branch";
    } else if (ref instanceof TagName) {
      refType = "tag";
    } else {
      refType = "named ref";
    }
    return new ReferenceConflictException(
        format(
            "Expected %s for %s '%s' but was %s",
            expectedArgument, refType, ref.getName(), actualArgument),
        t);
  }
}
