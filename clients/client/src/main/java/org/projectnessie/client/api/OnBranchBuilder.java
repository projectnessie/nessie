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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Validation;

/** Base interface for requests against a branch. */
public interface OnBranchBuilder<R extends OnBranchBuilder<R>> {
  R branchName(
      @NotNull @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName);

  R hash(
      @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash);

  /**
   * Convenience for {@link #branchName(String) branchName(branch.getName())}{@code .}{@link
   * #hash(String) hash(branch.getHash())}.
   */
  default R branch(Branch branch) {
    return branchName(branch.getName()).hash(branch.getHash());
  }
}
