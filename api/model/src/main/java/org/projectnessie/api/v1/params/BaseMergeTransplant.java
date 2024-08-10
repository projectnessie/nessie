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
package org.projectnessie.api.v1.params;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Validation;

public interface BaseMergeTransplant {

  @NotBlank
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  String getFromRefName();

  @SuppressWarnings("DeprecatedIsStillUsed")
  @Nullable
  @JsonInclude(Include.NON_NULL)
  @Deprecated
  Boolean keepIndividualCommits();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  List<MergeKeyBehavior> getKeyMergeModes();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  MergeBehavior getDefaultKeyMergeMode();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  Boolean isDryRun();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  Boolean isFetchAdditionalInfo();

  /**
   * When set to {@code true}, the {@code Merge} and {@code Transplant} operations will return
   * {@link MergeResponse} object when a content based conflict cannot be resolved, instead of
   * throwing a {@link org.projectnessie.error.NessieReferenceConflictException}.
   */
  @Nullable
  @JsonInclude(Include.NON_NULL)
  Boolean isReturnConflictAsResult();
}
