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

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.IdentifiedContentKey;

@Value.Immutable
public interface KeyEntry {

  IdentifiedContentKey getKey();

  @Nullable
  @jakarta.annotation.Nullable
  Content getContent();

  static ImmutableKeyEntry.Builder builder() {
    return ImmutableKeyEntry.builder();
  }

  static KeyEntry of(IdentifiedContentKey key) {
    return builder().key(key).build();
  }

  static KeyEntry of(
      IdentifiedContentKey key, @NotNull @jakarta.validation.constraints.NotNull Content content) {
    Preconditions.checkArgument(
        key.type().equals(content.getType()),
        "Content type from key '%s' does not match actual type of content: %s",
        key.type(),
        content);
    Preconditions.checkArgument(
        key.lastElement().contentId().equals(content.getId()),
        "Content id from key '%s' does not match actual id of content: %s",
        key,
        content);
    return builder().key(key).content(content).build();
  }
}
