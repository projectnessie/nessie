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
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "MultiGetContentRequest")
@Value.Immutable
@JsonSerialize(as = ImmutableMultiGetContentRequest.class)
@JsonDeserialize(as = ImmutableMultiGetContentRequest.class)
public interface MultiGetContentRequest {

  @NotNull
  @Size(min = 1)
  List<ContentKey> getRequestedKeys();

  static ImmutableMultiGetContentRequest.Builder builder() {
    return ImmutableMultiGetContentRequest.builder();
  }

  static MultiGetContentRequest of(ContentKey... keys) {
    return builder().addRequestedKeys(keys).build();
  }

  static MultiGetContentRequest of(List<ContentKey> keys) {
    return builder().addAllRequestedKeys(keys).build();
  }
}
