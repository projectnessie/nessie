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
package org.projectnessie.client.api;

import javax.validation.constraints.Pattern;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.Validation;

public interface MergeTransplantBuilder<R extends MergeTransplantBuilder<R>>
    extends OnBranchBuilder<R> {

  /** See javadoc on overriding methods for details. */
  R message(String message);

  R fromRefName(
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_REGEX,
              message = Validation.REF_NAME_MESSAGE)
          String fromRefName);

  R keepIndividualCommits(boolean keepIndividualCommits);

  R dryRun(boolean dryRun);

  R fetchAdditionalInfo(boolean fetchAdditionalInfo);

  R returnConflictAsResult(boolean returnConflictAsResult);

  R defaultMergeMode(MergeBehavior mergeBehavior);

  R mergeMode(ContentKey key, MergeBehavior mergeBehavior);

  R mergeKeyBehavior(MergeKeyBehavior mergeKeyBehavior);
}
