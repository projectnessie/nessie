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
package org.projectnessie.events.api;

import java.util.Optional;
import org.immutables.value.Value;

/**
 * A reference is a named pointer to a commit. Branches and tags are the most common types of
 * references, but more types may be added in the future.
 */
@Value.Immutable
public interface Reference {

  /** The reference type name for branches. */
  String BRANCH = "BRANCH";

  /** The reference type name for tags. */
  String TAG = "TAG";

  /**
   * The simple name of the reference, e.g. "branch1".
   *
   * <p>Simple names are ambiguous, e.g. "main" could be a branch ("refs/heads/main") or a tag
   * ("refs/tags/main"). Use {@link #getFullName()} to disambiguate simple names.
   */
  String getSimpleName();

  /**
   * The full name of the reference, e.g. "refs/heads/branch1".
   *
   * <p>Full names are unique across all reference types. If the reference was detached however,
   * this will return empty.
   *
   * <p>The reference type can be inferred from the full name; e.g. if the full name starts with
   * "refs/heads/", then the reference is a branch. If the full name starts with "refs/tags/", then
   * the reference is a tag.
   */
  Optional<String> getFullName();

  /**
   * The type of the reference. This is usually either {@value #BRANCH} or {@value #TAG}, but more
   * types may be added in the future.
   */
  String getType();

  /** Returns {@code true} if the reference is a branch, {@code false} otherwise. */
  @Value.Derived
  default boolean isBranch() {
    return getType().equalsIgnoreCase(BRANCH);
  }

  /** Returns {@code true} if the reference is a tag, {@code false} otherwise. */
  @Value.Derived
  default boolean isTag() {
    return getType().equalsIgnoreCase(TAG);
  }
}
