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

import javax.annotation.Nonnull;
import org.immutables.value.Value;

/** A pointer to a hash. */
@Value.Immutable
public interface TagName extends NamedRef {

  /**
   * Create a new tag reference.
   *
   * @param name the tag name
   * @return an instance of {@code TagName} for the provided name
   */
  @Nonnull
  static TagName of(@Nonnull String name) {
    return ImmutableTagName.builder().name(name).build();
  }
}
