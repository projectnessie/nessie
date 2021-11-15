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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "GetMultipleContentsResponse")
@Value.Immutable
@JsonSerialize(as = ImmutableGetMultipleContentsResponse.class)
@JsonDeserialize(as = ImmutableGetMultipleContentsResponse.class)
public interface GetMultipleContentsResponse {

  @NotNull
  List<ContentWithKey> getContents();

  static GetMultipleContentsResponse of(List<ContentWithKey> items) {
    return ImmutableGetMultipleContentsResponse.builder().addAllContents(items).build();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableContentWithKey.class)
  @JsonDeserialize(as = ImmutableContentWithKey.class)
  interface ContentWithKey {

    @NotNull
    ContentKey getKey();

    @NotNull
    Content getContent();

    static ContentWithKey of(ContentKey key, Content content) {
      return ImmutableContentWithKey.builder().key(key).content(content).build();
    }
  }
}
