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

/** Exception thrown when a reflog is not present in the store. */
public class RefLogNotFoundException extends VersionStoreException {
  private static final long serialVersionUID = -4782304450647510330L;

  public RefLogNotFoundException(String message) {
    super(message);
  }

  public RefLogNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a {@code RefLogNotFoundException} instance based on the provided reference.
   *
   * @param refLogId the refLogId string not found in the store
   * @return a {@code RefLogNotFoundException} instance
   * @throws NullPointerException if {@code refLogId} is {@code null}.
   */
  @Nonnull
  public static RefLogNotFoundException forRefLogId(@Nonnull String refLogId) {
    requireNonNull(refLogId);
    return new RefLogNotFoundException(format("RefLog entry for '%s' does not exist", refLogId));
  }
}
