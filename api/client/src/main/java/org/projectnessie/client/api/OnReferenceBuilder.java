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

import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX;

import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Validation;

/** Base interface for requests against a named reference, either a branch or tag. */
public interface OnReferenceBuilder<R extends OnReferenceBuilder<R>> {
  R refName(
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_REGEX,
              message = Validation.REF_NAME_MESSAGE)
          String refName);

  /**
   * Optional commit ID with an optional sequence of relative lookups. Relative lookups can be
   * by-timestamp, by-n-th-predecessor or by-n-th-parent.
   *
   * <p>Relative lookups are only supported for REST API {@link
   * NessieConfiguration#getActualApiVersion() v2} against servers announcing {@link
   * NessieConfiguration#getSpecVersion() Nessie specification} {@code 2.1.0} for most
   * functionalities.
   *
   * <ul>
   *   <li>Lookup by timestamp starts with {@code *} followed by the numeric value of the timestamp
   *       in milliseconds since epoch.
   *   <li>Lookup by n-th predecessor starts with {@code ~} followed by the value for the n-th
   *       commit in the commit log.
   *   <li>Lookup by n-th parent starts with {@code ^} followed by either 1, referencing the direct
   *       parent, or 2, referencing the merge parent.
   * </ul>
   *
   * <p>Valid values are:
   *
   * <ul>
   *   <li>{@code ~10} -> the 10th parent from the {@code HEAD}
   *   <li>{@code 11223344~10} -> the 10th parent of the commit {@code 11223344}
   *   <li>{@code 11223344^2} -> the merge parent of the commit {@code 11223344}
   *   <li>{@code 11223344~10^2} -> the merge parent of the 10th parent of the commit {@code
   *       11223344}
   *   <li>{@code 11223344~10^1} -> the direct parent of the 10th parent of the commit {@code
   *       11223344} - functionally equal to {@code 11223344~11}
   *   <li>{@code 11223344*10000000000} -> the commit in the commit log starting at {@code 11223344}
   *       with a commit-created timestamp of 10000000000 or less.
   *   <li>{@code *10000000000} -> the commit in the commit log starting at {@code HEAD} with a
   *       commit-created timestamp of 10000000000 or less.
   * </ul>
   */
  R hashOnRef(
      @Nullable
          @jakarta.annotation.Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hashOnRef);

  /**
   * Convenience for {@link #refName(String) refName(reference.getName())}{@code .}{@link
   * #hashOnRef(String) hashOnRef(reference.getHash())}.
   */
  default R reference(Reference reference) {
    return refName(reference.getName()).hashOnRef(reference.getHash());
  }
}
