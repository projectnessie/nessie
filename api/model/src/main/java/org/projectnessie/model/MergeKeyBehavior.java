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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Value.Immutable
@JsonSerialize(as = ImmutableMergeKeyBehavior.class)
@JsonDeserialize(as = ImmutableMergeKeyBehavior.class)
public interface MergeKeyBehavior {

  ContentKey getKey();

  MergeBehavior getMergeBehavior();

  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Content getExpectedSourceContent();

  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Content getExpectedTargetContent();

  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Content getResolvedContent();

  static ImmutableMergeKeyBehavior.Builder builder() {
    return ImmutableMergeKeyBehavior.builder();
  }

  static MergeKeyBehavior of(ContentKey key, MergeBehavior mergeBehavior) {
    return builder().key(key).mergeBehavior(mergeBehavior).build();
  }
}
