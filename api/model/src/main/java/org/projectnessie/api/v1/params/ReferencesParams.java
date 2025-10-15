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
package org.projectnessie.api.v1.params;

import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.api.v1.http.HttpTreeApi;
import org.projectnessie.model.FetchOption;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@link
 * HttpTreeApi#getAllReferences(ReferencesParams)}.
 *
 * <p>For easier usage of this class, there is {@link ReferencesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class ReferencesParams extends AbstractParams<ReferencesParams> {

  @Parameter(
      description =
          "Specify how much information to be returned. Will fetch additional metadata for references if set to 'ALL'.\n\n"
              + "A returned Branch instance will have the following information:\n\n"
              + "- numCommitsAhead (number of commits ahead of the default branch)\n\n"
              + "- numCommitsBehind (number of commits behind the default branch)\n\n"
              + "- commitMetaOfHEAD (the commit metadata of the HEAD commit)\n\n"
              + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
              + "- numTotalCommits (the total number of commits in this reference).\n\n"
              + "A returned Tag instance will only contain the 'commitMetaOfHEAD' and 'numTotalCommits' fields.\n\n"
              + "Note that computing & fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care.")
  @QueryParam("fetch")
  @jakarta.ws.rs.QueryParam("fetch")
  @Nullable
  @jakarta.annotation.Nullable
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
  @jakarta.ws.rs.QueryParam("filter")
  @Nullable
  @jakarta.annotation.Nullable
  private String filter;

  public ReferencesParams() {}

  @org.immutables.builder.Builder.Constructor
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
  public FetchOption fetchOption() {
    return fetchOption;
  }

  @Nullable
  @jakarta.annotation.Nullable
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

  @Override
  public String toString() {
    return new StringJoiner(", ", ReferencesParams.class.getSimpleName() + "[", "]")
        .add("maxRecords=" + maxRecords())
        .add("pageToken='" + pageToken() + "'")
        .add("fetchOption=" + fetchOption)
        .add("filter=" + filter)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReferencesParams)) {
      return false;
    }
    ReferencesParams that = (ReferencesParams) o;
    return Objects.equals(maxRecords(), that.maxRecords())
        && Objects.equals(pageToken(), that.pageToken())
        && fetchOption == that.fetchOption
        && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxRecords(), pageToken(), fetchOption, filter);
  }
}
