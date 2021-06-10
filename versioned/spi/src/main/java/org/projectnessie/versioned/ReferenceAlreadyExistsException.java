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

import javax.annotation.Nonnull;

/** Exception thrown when a reference already exists in the store. */
public class ReferenceAlreadyExistsException extends VersionStoreException {
  private static final long serialVersionUID = -6125198004202778224L;

  public ReferenceAlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Create a {@code ReferenceConflictException} instance with an accurate message based on the
   * provided named referenced and the compared hashes.
   *
   * @param ref the named reference
   * @return a {@code ReferenceNotFoundException} instance
   * @throws NullPointerException if {@code ref} is {@code null}.
   */
  @Nonnull
  public static ReferenceAlreadyExistsException forReference(@Nonnull NamedRef ref) {
    final String refType;
    if (ref instanceof BranchName) {
      refType = "branch";
    } else if (ref instanceof TagName) {
      refType = "tag";
    } else {
      refType = "named ref";
    }
    return new ReferenceAlreadyExistsException(
        format("%s '%s' already exists", refType, ref.getName()));
  }
}
