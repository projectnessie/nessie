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

import static org.projectnessie.api.v2.doc.ApiDoc.REFERENCES_FETCH_OPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.REFERENCES_FILTER;

import com.fasterxml.jackson.annotation.JsonInclude;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.FetchOption;

public interface ReferencesSpec extends AbstractSpec {

  @Parameter(description = REFERENCES_FETCH_OPTION)
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  FetchOption fetchOption();

  @Parameter(
      description = REFERENCES_FILTER,
      examples = {
        @ExampleObject(ref = "expr_by_refType"),
        @ExampleObject(ref = "expr_by_ref_name"),
        @ExampleObject(ref = "expr_by_ref_commit")
      })
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String filter();
}
