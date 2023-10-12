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

import static org.projectnessie.api.v2.doc.ApiDoc.REF_GET_PARAMETER_DESCRIPTION;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.model.Validation;

public class ReferenceHistoryParams {

  @Parameter(
      description = REF_GET_PARAMETER_DESCRIPTION,
      examples = {@ExampleObject(ref = "ref"), @ExampleObject(ref = "refDefault")})
  @PathParam("ref")
  @jakarta.ws.rs.PathParam("ref")
  @NotNull
  @jakarta.validation.constraints.NotNull
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_PATH_REGEX,
      message = Validation.REF_NAME_MESSAGE)
  private String ref;

  @Parameter(
      description =
          "Optional parameter, specifies the number of commits to scan from the reference's current HEAD, "
              + "limited to the given amount of commits. Default is to not scan the commit log. The server "
              + "may impose a hard limit on the amount of commits from the commit log.")
  @QueryParam("scan-commits")
  @jakarta.ws.rs.QueryParam("scan-commits")
  @Nullable
  @jakarta.annotation.Nullable
  private Integer headCommitsToScan;

  public ReferenceHistoryParams() {}

  @Constructor
  ReferenceHistoryParams(
      @NotNull @jakarta.validation.constraints.NotNull String ref,
      @Nullable @jakarta.annotation.Nullable Integer headCommitsToScan) {
    this.ref = ref;
    this.headCommitsToScan = headCommitsToScan;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public Integer headCommitsToScan() {
    return headCommitsToScan;
  }

  public String getRef() {
    return ref;
  }

  public static ReferenceHistoryParamsBuilder builder() {
    return new ReferenceHistoryParamsBuilder();
  }
}
