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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Value.Immutable
@JsonSerialize(as = ImmutableEntriesResponse.class)
@JsonDeserialize(as = ImmutableEntriesResponse.class)
public interface EntriesResponse extends PaginatedResponse {

  static ImmutableEntriesResponse.Builder builder() {
    return ImmutableEntriesResponse.builder();
  }

  @NotNull
  List<Entry> getEntries();

  @Value.Immutable
  @JsonSerialize(as = ImmutableEntry.class)
  @JsonDeserialize(as = ImmutableEntry.class)
  interface Entry {

    static ImmutableEntry.Builder builder() {
      return ImmutableEntry.builder();
    }

    @NotNull
    @Value.Parameter(order = 2)
    Content.Type getType();

    @NotNull
    @Value.Parameter(order = 1)
    ContentKey getName();

    @JsonView(Views.V2.class)
    @Value.Parameter(order = 3)
    @Nullable // for V1 backwards compatibility
    String getContentId();

    static Entry entry(ContentKey name, Content.Type type, String contentId) {
      return ImmutableEntry.of(name, type, contentId);
    }
  }
}
