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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** A {@link Content} object that represents a namespace. */
@Value.Immutable
@JsonTypeName("NAMESPACE")
@JsonSerialize
@JsonDeserialize
public interface Namespace extends Content {

  @Override
  @Value.Default
  default ContentType getType() {
    return ContentType.NAMESPACE;
  }

  /**
   * The name of the namespace.
   *
   * <p>The name is composed of the elements of the namespace, separated by dots. For example, the
   * name of the namespace {@code ["a", "b", "c"]} is {@code "a.b.c"}.
   */
  @Value.Lazy
  @JsonIgnore
  default String getName() {
    // see org.projectnessie.model.Util
    return getElements().stream()
        .map(element -> element.replace('.', '\u001D').replace('\u0000', '\u001D'))
        .collect(Collectors.joining("."));
  }

  List<String> getElements();
}
