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

import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.api.params.FetchOption;

/**
 * The purpose of this class is to include optional parameters that can be passed to API methods
 * dealing with reference retrieval.
 *
 * <p>For easier usage of this class, there is {@link ReferencesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class ReferencesParams extends AbstractParams<ReferencesParams> {

  @Parameter(
      description =
          "Specifies how much extra information is to be retrived from the server.\n\n"
              + "If the fetch option is set to 'ALL' the following addition information will be returned for each "
              + "Branch object in the output:\n\n"
              + "- numCommitsAhead (number of commits ahead of the default branch)\n\n"
              + "- numCommitsBehind (number of commits behind the default branch)\n\n"
              + "- commitMetaOfHEAD (the commit metadata of the HEAD commit)\n\n"
              + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
              + "- numTotalCommits (the total number of commits from the root to the HEAD of the branch).\n\n"
              + "The returned Tag instances will only contain the 'commitMetaOfHEAD' and 'numTotalCommits' fields.\n\n"
              + "Note that computing & fetching additional metadata might be computationally expensive on the "
              + "server-side, so this flag should be used with care.")
  @QueryParam("fetch")
  @Nullable
  private FetchOption fetchOption;

  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
              + "Usable variables within the expression are:\n\n"
              + "- ref (Reference) describes the reference, with fields name (String), hash (String), metadata (ReferenceMetadata)\n\n"
              + "- metadata (ReferenceMetadata) shortcut to ref.metadata, never null, but possibly empty\n\n"
              + "- commit (CommitMeta) - shortcut to ref.metadata.commitMetaOfHEAD, never null, but possibly empty\n\n"
              + "- refType (String) - the reference type, either BRANCH or TAG\n\n"
              + "Note that the expression can only test attributes metadata and commit, if 'fetchOption' is set to 'ALL'.",
      examples = {
        @ExampleObject(ref = "expr_by_refType"),
        @ExampleObject(ref = "expr_by_ref_name"),
        @ExampleObject(ref = "expr_by_ref_commit")
      })
  @QueryParam("filter")
  @Nullable
  private String filter;

  public ReferencesParams() {}

  @Constructor
  ReferencesParams(
      @Nullable Integer maxRecords,
      @Nullable String pageToken,
      @Nullable FetchOption fetchOption,
      @Nullable String filter) {
    super(maxRecords, pageToken);
    this.fetchOption = fetchOption;
    this.filter = filter;
  }

  @Nullable
  public FetchOption fetchOption() {
    return fetchOption;
  }

  @Nullable
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
