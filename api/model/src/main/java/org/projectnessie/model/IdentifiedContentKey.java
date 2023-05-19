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
package org.projectnessie.model;

import static org.projectnessie.model.IdentifiedContentKey.IdentifiedElement.identifiedElement;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableIdentifiedContentKey.class)
@JsonDeserialize(as = ImmutableIdentifiedContentKey.class)
public interface IdentifiedContentKey {
  @NotNull
  @jakarta.validation.constraints.NotNull
  @Size
  @jakarta.validation.constraints.Size(min = 1)
  List<IdentifiedElement> elements();

  @NotNull
  @jakarta.validation.constraints.NotNull
  ContentKey contentKey();

  @JsonIgnore
  @Value.Redacted
  default IdentifiedElement lastElement() {
    List<IdentifiedElement> el = elements();
    return el.get(el.size() - 1);
  }

  @Nullable
  @jakarta.annotation.Nullable
  Content.Type type();

  @Value.Check
  default void check() {
    if (contentKey().getElementCount() != elements().size()) {
      throw new IllegalArgumentException(
          "Number of content-key elements must match the identified-elements");
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableIdentifiedElement.class)
  @JsonDeserialize(as = ImmutableIdentifiedElement.class)
  interface IdentifiedElement {
    @Value.Parameter(order = 1)
    String element();

    @Value.Parameter(order = 2)
    @Nullable
    @jakarta.annotation.Nullable
    String contentId();

    static IdentifiedElement identifiedElement(String element, String contentId) {
      return ImmutableIdentifiedElement.of(element, contentId);
    }
  }

  static ImmutableIdentifiedContentKey.Builder builder() {
    return ImmutableIdentifiedContentKey.builder();
  }

  static IdentifiedContentKey identifiedContentKeyFromContent(
      ContentKey key, Content content, Function<List<String>, String> keyToContentId) {
    return identifiedContentKeyFromContent(key, content.getType(), content.getId(), keyToContentId);
  }

  static IdentifiedContentKey identifiedContentKeyFromContent(
      ContentKey key,
      Content.Type type,
      String contentId,
      Function<List<String>, String> keyToContentId) {
    ImmutableIdentifiedContentKey.Builder identifiedKey =
        IdentifiedContentKey.builder().contentKey(key).type(type);
    List<String> path = new ArrayList<>();
    List<String> keyElements = key.getElements();
    int elementCount = keyElements.size();
    for (int i = 0; i < elementCount - 1; i++) {
      String element = keyElements.get(i);
      path.add(element);
      String id = keyToContentId.apply(path);
      identifiedKey.addElements(identifiedElement(element, id));
    }
    identifiedKey.addElements(identifiedElement(keyElements.get(elementCount - 1), contentId));
    return identifiedKey.build();
  }
}
