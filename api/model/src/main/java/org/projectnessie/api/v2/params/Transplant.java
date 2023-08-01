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
import static org.projectnessie.model.Validation.validateHashOrRelativeSpec;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;
import org.projectnessie.model.Validation;

@Schema(
    type = SchemaType.OBJECT,
    title = "Transplant",
    // Smallrye does neither support JsonFormat nor javax.validation.constraints.Pattern :(
    properties = {
      @SchemaProperty(
          name = "message",
          description = "Commit message for this transplant request."),
      @SchemaProperty(
          name = "hashesToTransplant",
          uniqueItems = true,
          description =
              "Lists the hashes of commits that should be transplanted into the target branch."),
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
@JsonSerialize(as = ImmutableTransplant.class)
@JsonDeserialize(as = ImmutableTransplant.class)
public interface Transplant extends BaseMergeTransplant {

  @Override
  @Nullable
  @jakarta.annotation.Nullable
  @Size
  @jakarta.validation.constraints.Size(min = 1)
  String getMessage();

  /**
   * The hashes of commits that should be transplanted into the target branch.
   *
   * <p>Since Nessie spec 2.1.1, hashes can be absolute or relative.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  @Size
  @jakarta.validation.constraints.Size(min = 1)
  List<
          @Pattern(
              regexp = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String>
      getHashesToTransplant();

  /**
   * Validation rule using {@link
   * org.projectnessie.model.Validation#validateHashOrRelativeSpec(String)} (String)}.
   */
  @Value.Check
  default void checkHashes() {
    List<String> hashes = getHashesToTransplant();
    if (hashes != null) {
      for (String hash : hashes) {
        validateHashOrRelativeSpec(hash);
      }
    }
  }
}
