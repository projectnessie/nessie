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
package org.projectnessie.api.v2.params;

import static org.projectnessie.api.v2.doc.ApiDoc.DIFF_FILTER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.REF_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.REQUESTED_KEY_PARAMETER_DESCRIPTION;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.ContentKey;

public interface DiffParamsSpec extends KeyRangeSpec {

  @Parameter(
      description = REF_PARAMETER_DESCRIPTION,
      examples = {
        @ExampleObject(ref = "ref"),
        @ExampleObject(ref = "refWithHash"),
        @ExampleObject(
            ref = "refWithTimestampMillisSinceEpoch",
            description =
                "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
        @ExampleObject(
            ref = "refWithTimestampInstant",
            description = "The commit 'valid for' the given ISO-8601 instant on main"),
        @ExampleObject(
            ref = "refWithNthPredecessor",
            description = "The 10th commit from HEAD of main"),
        @ExampleObject(
            ref = "refWithMergeParent",
            description =
                "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
        @ExampleObject(ref = "refDefault"),
        @ExampleObject(ref = "refDetached"),
      })
  @Nullable
  @jakarta.annotation.Nullable
  String getFromRef();

  @Parameter(
      description =
          "Same reference spec as in the 'from-ref' parameter but identifying the other tree for comparison.")
  @Nullable
  @jakarta.annotation.Nullable
  String getToRef();

  @Parameter(description = REQUESTED_KEY_PARAMETER_DESCRIPTION)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Nullable
  @jakarta.annotation.Nullable
  List<ContentKey> getRequestedKeys();

  @Parameter(description = DIFF_FILTER_DESCRIPTION)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String getFilter();
}
