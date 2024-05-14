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
import java.util.List;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Value.Immutable
@JsonSerialize(as = ImmutableMergeKeyBehavior.class)
@JsonDeserialize(as = ImmutableMergeKeyBehavior.class)
public interface MergeKeyBehavior {

  ContentKey getKey();

  MergeBehavior getMergeBehavior();

  /**
   * If present, the current content on the target branch will be compared against this value.
   *
   * <p>This parameter is not supported when multiple commits will be generated, which means only
   * merge operations.
   *
   * <p>Supplying a {@linkplain #getResolvedContent() resolved content} requires setting this
   * attribute. The merge operation will result in a "conflict", if current value on the target
   * branch is different from this value.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Content getExpectedTargetContent();

  /**
   * Clients can provide a "resolved" content object, which will then automatically be persisted via
   * the merge operation instead of detecting and potentially raising a merge-conflict, assuming the
   * content-type is the same.
   *
   * <p>This parameter is not supported when multiple commits will be generated, which means only
   * merge operations.
   *
   * <p>It is mandatory to supply the {@linkplain #getExpectedTargetContent() expected content
   * value}.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Content getResolvedContent();

  /**
   * If present, the current documentation on the target branch will be compared against this value.
   *
   * <p>This parameter is not supported when multiple commits will be generated, which means only
   * merge operations.
   *
   * <p>Supplying a {@linkplain #getResolvedDocumentation() resolved documentation} requires setting
   * this attribute. The merge operation will result in a "conflict", if current value on the target
   * branch is different from this value.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Documentation getExpectedTargetDocumentation();

  /**
   * Clients can provide a "resolved" documentation object, which will then automatically be
   * persisted via the merge operation instead of detecting and potentially raising a
   * merge-conflict, assuming the content-type is the same.
   *
   * <p>This parameter is not supported when multiple commits will be generated, which means only
   * merge operations.
   *
   * <p>It is mandatory to supply the {@linkplain #getExpectedTargetDocumentation() expected
   * documentation value}.
   */
  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  Documentation getResolvedDocumentation();

  /**
   * Additional information about the operation and/or content object. If and how a Nessie server
   * uses and handles the information depends on the server version and type of metadata (called
   * variant).
   */
  @JsonInclude(Include.NON_EMPTY)
  @JsonView(Views.V2.class)
  List<ContentMetadata> getMetadata();

  static ImmutableMergeKeyBehavior.Builder builder() {
    return ImmutableMergeKeyBehavior.builder();
  }

  static MergeKeyBehavior of(ContentKey key, MergeBehavior mergeBehavior) {
    return builder().key(key).mergeBehavior(mergeBehavior).build();
  }

  static MergeKeyBehavior of(
      ContentKey key,
      MergeBehavior mergeBehavior,
      Content expectedTargetContent,
      Content resolvedContent) {
    return builder()
        .key(key)
        .mergeBehavior(mergeBehavior)
        .expectedTargetContent(expectedTargetContent)
        .resolvedContent(resolvedContent)
        .build();
  }
}
