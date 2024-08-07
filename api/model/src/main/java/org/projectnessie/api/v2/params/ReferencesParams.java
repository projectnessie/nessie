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

import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.model.FetchOption;

/**
 * The purpose of this class is to include optional parameters that can be passed to API methods
 * dealing with reference retrieval.
 *
 * <p>For easier usage of this class, there is {@link ReferencesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class ReferencesParams extends AbstractParams<ReferencesParams> implements ReferencesSpec {

  @Parameter(description = REFERENCES_FETCH_OPTION)
  @QueryParam("fetch")
  @jakarta.ws.rs.QueryParam("fetch")
  @Nullable
  @jakarta.annotation.Nullable
  private FetchOption fetchOption;

  @Parameter(
      description = REFERENCES_FILTER,
      examples = {
        @ExampleObject(ref = "expr_by_refType"),
        @ExampleObject(ref = "expr_by_ref_name"),
        @ExampleObject(ref = "expr_by_ref_commit")
      })
  @QueryParam("filter")
  @jakarta.ws.rs.QueryParam("filter")
  @Nullable
  @jakarta.annotation.Nullable
  private String filter;

  public ReferencesParams() {}

  @Constructor
  ReferencesParams(
      @Nullable @jakarta.annotation.Nullable Integer maxRecords,
      @Nullable @jakarta.annotation.Nullable String pageToken,
      @Nullable @jakarta.annotation.Nullable FetchOption fetchOption,
      @Nullable @jakarta.annotation.Nullable String filter) {
    super(maxRecords, pageToken);
    this.fetchOption = fetchOption;
    this.filter = filter;
  }

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  public FetchOption fetchOption() {
    return fetchOption;
  }

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  public String filter() {
    return filter;
  }

  public static ReferencesParamsBuilder builder() {
    return new ReferencesParamsBuilder();
  }

  public static ReferencesParams empty() {
    return builder().build();
  }

  @Override
  public ReferencesParams forNextPage(String pageToken) {
    return new ReferencesParams(maxRecords(), pageToken, fetchOption, filter);
  }
}
