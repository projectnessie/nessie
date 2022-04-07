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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import org.immutables.value.Value;

public interface BaseMergeTransplant {

  @NotBlank
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  String getFromRefName();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  Boolean keepIndividualCommits();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  List<MergeKeyBehavior> getKeyMergeModes();

  @Nullable
  @JsonInclude(Include.NON_NULL)
  MergeBehavior getDefaultKeyMergeMode();

  @Value.Immutable
  @JsonSerialize(as = ImmutableMergeKeyBehavior.class)
  @JsonDeserialize(as = ImmutableMergeKeyBehavior.class)
  interface MergeKeyBehavior {
    ContentKey getKey();

    MergeBehavior getMergeBehavior();

    static ImmutableMergeKeyBehavior.Builder builder() {
      return ImmutableMergeKeyBehavior.builder();
    }

    static MergeKeyBehavior of(ContentKey key, MergeBehavior mergeBehavior) {
      return builder().key(key).mergeBehavior(mergeBehavior).build();
    }
  }

  enum MergeBehavior {
    /** Keys with this merge mode will be merged, conflict detection takes place. */
    NORMAL,
    /** Keys with this merge mode will be merged unconditionally, no conflict detection. */
    FORCE,
    /** Keys with this merge mode will not be merged. */
    DROP
  }
}
