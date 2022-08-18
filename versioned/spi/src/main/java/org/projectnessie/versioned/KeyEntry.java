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
import org.projectnessie.model.Content;

@Value.Immutable
public interface KeyEntry {

  /** Get the type of this entity. */
  Content.Type getType();

  Key getKey();

  String getContentId();

  static ImmutableKeyEntry.Builder builder() {
    return ImmutableKeyEntry.builder();
  }

  static KeyEntry of(Content.Type type, Key key, String contentId) {
    return builder().type(type).key(key).contentId(contentId).build();
  }
}
