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
import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnull;

/** Exception thrown when a reference is not present in the store. */
public class ReferenceNotFoundException extends VersionStoreException {
  private static final long serialVersionUID = -4231207387427624751L;

  public ReferenceNotFoundException(String message) {
    super(message);
  }

  public ReferenceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a {@code ReferenceNotFoundException} instance with an accurate message based on the
   * provided reference.
   *
   * @param ref the reference not found in the store
   * @return a {@code ReferenceNotFoundException} instance
   * @throws NullPointerException if {@code ref} is {@code null}.
   */
  @Nonnull
  public static ReferenceNotFoundException forReference(@Nonnull Ref ref) {
    requireNonNull(ref);

    final String message;
    if (ref instanceof BranchName) {
      message = format("Branch '%s' does not exist", ((BranchName) ref).getName());
    } else if (ref instanceof TagName) {
      message = format("Tag '%s' does not exist", ((TagName) ref).getName());
    } else if (ref instanceof Hash) {
      message = format("Hash '%s' does not exist", ((Hash) ref).asString());
    } else {
      return forReference(ref.toString());
    }
    return new ReferenceNotFoundException(message);
  }

  /**
   * Create a {@code ReferenceNotFoundException} instance based on the provided reference.
   *
   * @param ref the reference string not found in the store
   * @return a {@code ReferenceNotFoundException} instance
   * @throws NullPointerException if {@code ref} is {@code null}.
   */
  @Nonnull
  public static ReferenceNotFoundException forReference(@Nonnull String ref) {
    requireNonNull(ref);
    return new ReferenceNotFoundException(format("Ref '%s' does not exist", ref));
  }
}
