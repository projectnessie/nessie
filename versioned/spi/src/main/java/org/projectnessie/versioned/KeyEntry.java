/*
 * Copyright (C) 2022 Dremio
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

import org.immutables.value.Value;

@Value.Immutable
public interface KeyEntry<TYPE extends Enum<TYPE>> {

  /** Get the type of this entity as an enum. */
  Enum<TYPE> getType();

  Key getKey();

  String getContentId();

  static <T extends Enum<T>> ImmutableKeyEntry.Builder<T> builder() {
    return ImmutableKeyEntry.builder();
  }

  static <T extends Enum<T>> KeyEntry<T> of(T type, Key key, String contentId) {
    ImmutableKeyEntry.Builder<T> builder = builder();
    return builder.type(type).key(key).contentId(contentId).build();
  }
}
