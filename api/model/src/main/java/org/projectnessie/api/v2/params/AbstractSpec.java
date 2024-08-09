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

import static org.projectnessie.api.v2.doc.ApiDoc.MAX_RECORDS;
import static org.projectnessie.api.v2.doc.ApiDoc.PAGE_TOKEN;

import com.fasterxml.jackson.annotation.JsonInclude;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

public interface AbstractSpec {

  @Parameter(description = MAX_RECORDS)
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer maxRecords();

  @Parameter(description = PAGE_TOKEN)
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String pageToken();
}
