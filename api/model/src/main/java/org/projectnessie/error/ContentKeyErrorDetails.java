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
package org.projectnessie.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;

@Value.Immutable
@JsonSerialize(as = ImmutableContentKeyErrorDetails.class)
@JsonDeserialize(as = ImmutableContentKeyErrorDetails.class)
@JsonTypeName(ContentKeyErrorDetails.TYPE)
public interface ContentKeyErrorDetails extends NessieErrorDetails {

  String TYPE = "CONTENT_KEY";

  @Override
  default String getType() {
    return TYPE;
  }

  @JsonInclude(Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 1)
  ContentKey contentKey();

  static ContentKeyErrorDetails contentKeyErrorDetails(
      @Nullable @jakarta.annotation.Nullable ContentKey contentKey) {
    return ImmutableContentKeyErrorDetails.of(contentKey);
  }
}
