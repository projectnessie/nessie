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

import static org.projectnessie.api.v2.doc.ApiDoc.ENTRIES_FILTER;
import static org.projectnessie.api.v2.doc.ApiDoc.REQUESTED_KEY_PARAMETER_DESCRIPTION;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.ContentKey;

public interface EntriesParamsSpec extends KeyRangeSpec {

  @Parameter(description = REQUESTED_KEY_PARAMETER_DESCRIPTION)
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  List<ContentKey> getRequestedKeys();

  @Parameter(
      description = ENTRIES_FILTER,
      examples = {
        @ExampleObject(ref = "expr_by_namespace"),
        @ExampleObject(ref = "expr_by_contentType"),
        @ExampleObject(ref = "expr_by_namespace_and_contentType")
      })
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String filter();

  @Parameter(description = "Optionally request to return 'Content' objects for the returned keys.")
  boolean withContent();
}
