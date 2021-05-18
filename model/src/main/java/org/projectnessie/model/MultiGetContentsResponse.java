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
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "MultiGetContentsResponse")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableMultiGetContentsResponse.class)
@JsonDeserialize(as = ImmutableMultiGetContentsResponse.class)
public interface MultiGetContentsResponse extends Base {

  List<ContentsWithKey> getContents();

  static MultiGetContentsResponse of(List<ContentsWithKey> items) {
    return ImmutableMultiGetContentsResponse.builder().addAllContents(items).build();
  }

  @Value.Immutable(prehash = true)
  @JsonSerialize(as = ImmutableContentsWithKey.class)
  @JsonDeserialize(as = ImmutableContentsWithKey.class)
  interface ContentsWithKey extends Base {

    ContentsKey getKey();

    Contents getContents();

    static ContentsWithKey of(ContentsKey key, Contents contents) {
      return ImmutableContentsWithKey.builder().key(key).contents(contents).build();
    }
  }
}
