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
package org.projectnessie.api;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.Validation;

public interface ContentApi {

  // Note: When substantial changes in Nessie API (this and related interfaces) are made
  // the API version number reported by NessieConfiguration.getMaxSupportedApiVersion()
  // should be increased as well.

  /**
   * This operation returns the {@link Content} for a {@link ContentKey} in a named-reference (a
   * {@link org.projectnessie.model.Branch} or {@link org.projectnessie.model.Tag}).
   *
   * <p>If the table-metadata is tracked globally (Iceberg), Nessie returns a {@link Content}
   * object, that contains the most up-to-date part for the globally tracked part (Iceberg:
   * table-metadata) plus the per-Nessie-reference/hash specific part (Iceberg: snapshot-ID,
   * schema-ID, partition-spec-ID, default-sort-order-ID).
   *
   * @param key the {@link ContentKey}s to retrieve
   * @param ref named-reference to retrieve the content for
   * @param hashOnRef hash on {@code ref} to retrieve the content for, translates to {@code HEAD},
   *     if missing/{@code null}
   * @return list of {@link ContentWithKey}s
   * @throws NessieNotFoundException if {@code ref} or {@code hashOnRef} does not exist
   */
  Content getContent(
      @Valid ContentKey key,
      @Valid @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String ref,
      @Valid @Nullable @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hashOnRef)
      throws NessieNotFoundException;

  /**
   * Similar to {@link #getContent(ContentKey, String, String)}, but takes multiple {@link
   * ContentKey}s and returns the {@link Content} for the one or more {@link ContentKey}s in a
   * named-reference (a {@link org.projectnessie.model.Branch} or {@link
   * org.projectnessie.model.Tag}).
   *
   * <p>If the table-metadata is tracked globally (Iceberg), Nessie returns a {@link Content}
   * object, that contains the most up-to-date part for the globally tracked part (Iceberg:
   * table-metadata) plus the per-Nessie-reference/hash specific part (Iceberg: snapshot-ID,
   * schema-ID, partition-spec-ID, default-sort-order-ID).
   *
   * @param ref named-reference to retrieve the content for
   * @param hashOnRef hash on {@code ref} to retrieve the content for, translates to {@code HEAD},
   *     if missing/{@code null}
   * @param request the {@link ContentKey}s to retrieve
   * @return list of {@link ContentWithKey}s
   * @throws NessieNotFoundException if {@code ref} or {@code hashOnRef} does not exist
   */
  GetMultipleContentsResponse getMultipleContents(
      @Valid @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String ref,
      @Valid @Nullable @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hashOnRef,
      @Valid @NotNull GetMultipleContentsRequest request)
      throws NessieNotFoundException;
}
