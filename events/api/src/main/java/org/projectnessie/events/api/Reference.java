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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * A reference is a named pointer to a commit. Branches and tags are the most common types of
 * references, but more types may be added in the future.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableReference.class)
@JsonDeserialize(as = ImmutableReference.class)
public interface Reference {

  /** The reference type name for branches. */
  String BRANCH = "BRANCH";

  /** The reference type name for tags. */
  String TAG = "TAG";

  /** The name of the reference, e.g. "branch1". */
  String getSimpleName();

  /** The full name of the reference, e.g. "refs/heads/branch1". */
  String getFullName();

  /**
   * The type of the reference. This is usually either {@value #BRANCH} or {@value #TAG}, but more
   * types may be added in the future.
   */
  String getType();

  /** Returns {@code true} if the reference is a branch, {@code false} otherwise. */
  @Value.Derived
  @JsonIgnore
  default boolean isBranch() {
    return getType().equalsIgnoreCase(BRANCH);
  }

  /** Returns {@code true} if the reference is a tag, {@code false} otherwise. */
  @Value.Derived
  @JsonIgnore
  default boolean isTag() {
    return getType().equalsIgnoreCase(TAG);
  }
}
