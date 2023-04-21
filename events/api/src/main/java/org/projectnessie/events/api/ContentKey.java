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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/** Key for a {@link Content} object. */
@Value.Immutable
@JsonSerialize
@JsonDeserialize(as = ImmutableContentKey.class)
public interface ContentKey {

  static ContentKey of(String... elements) {
    return ImmutableContentKey.of(Arrays.asList(elements));
  }

  @Value.Parameter
  List<String> getElements();

  @Value.Lazy
  @JsonIgnore
  default String getName() {
    return getElements().get(getElements().size() - 1);
  }

  @Value.Lazy
  @JsonIgnore
  default Optional<ImmutableContentKey> getParent() {
    List<String> elements = getElements();
    if (elements.size() <= 1) {
      return Optional.empty();
    }
    return Optional.of(ImmutableContentKey.of(elements.subList(0, elements.size() - 1)));
  }
}
