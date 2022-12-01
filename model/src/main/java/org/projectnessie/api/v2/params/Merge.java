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
package org.projectnessie.api.v2.params;

import static org.projectnessie.api.v2.doc.ApiDoc.DEFAULT_KEY_MERGE_MODE_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.DRY_RUN_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.FETCH_ADDITION_INFO_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.FROM_REF_NAME_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.KEY_MERGE_MODES_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.RETURN_CONFLICTS_AS_RESULT_DESCRIPTION;
import static org.projectnessie.model.Validation.validateHash;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;
import org.projectnessie.model.Validation;

@Schema(
    title = "Merge Operation",
    properties = {
      @SchemaProperty(
          name = "message",
          description =
              "Optional commit message for this merge request\n"
                  + "\n"
                  + "If not set, the server will generate a commit message automatically using metadata from the \n"
                  + "merged commits."),
      @SchemaProperty(
          name = "fromHash",
          pattern = Validation.HASH_REGEX,
          description =
              "The hash of the last commit to merge.\n"
                  + "\n"
                  + "This commit must be present in the history on 'fromRefName' before the first common parent with respect "
                  + "to the target branch."),
      @SchemaProperty(name = "fromRefName", description = FROM_REF_NAME_DESCRIPTION),
      @SchemaProperty(name = "keyMergeModes", description = KEY_MERGE_MODES_DESCRIPTION),
      @SchemaProperty(
          name = "defaultKeyMergeMode",
          description = DEFAULT_KEY_MERGE_MODE_DESCRIPTION),
      @SchemaProperty(name = "dryRun", description = DRY_RUN_DESCRIPTION),
      @SchemaProperty(name = "fetchAdditionalInfo", description = FETCH_ADDITION_INFO_DESCRIPTION),
      @SchemaProperty(
          name = "returnConflictAsResult",
          description = RETURN_CONFLICTS_AS_RESULT_DESCRIPTION),
    })
@Value.Immutable
@JsonSerialize(as = ImmutableMerge.class)
@JsonDeserialize(as = ImmutableMerge.class)
public interface Merge extends BaseMergeTransplant {

  @Override
  @Nullable
  @Size(min = 1)
  String getMessage();

  @NotBlank
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  String getFromHash();

  /**
   * Validation rule using {@link org.projectnessie.model.Validation#validateHash(String)}
   * (String)}.
   */
  @Value.Check
  default void checkHash() {
    String hash = getFromHash();
    if (hash != null) {
      validateHash(hash);
    }
  }
}
