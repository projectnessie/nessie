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
package org.projectnessie.api.params;

import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@code
 * HttpTreeApi#getCommitLog(String, CommitLogParams)}.
 *
 * <p>For easier usage of this class, there is {@link CommitLogParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class CommitLogParams extends AbstractParams {

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description =
          "Hash on the given ref to start from (in chronological sense), the 'far' end of the commit log, returned 'late' in the result.",
      examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
  @QueryParam("startHash")
  private String startHash;

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description =
          "Hash on the given ref to end at (in chronological sense), the 'near' end of the commit log, returned 'early' in the result.",
      examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
  @QueryParam("endHash")
  private String endHash;

  @Nullable
  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n\n"
              + "Usable variables within the expression are:\n\n"
              + "- 'commit' with fields 'author' (string), 'committer' (string), 'commitTime' (timestamp), 'hash' (string), ',message' (string), 'properties' (map)\n\n"
              + "- 'operations' (list), each operation has the fields 'type' (string, either 'PUT' or 'DELETE'), 'key' (string, namespace + table name), 'keyElements' (list of strings), 'namespace' (string), 'namespaceElements' (list of strings) and 'name' (string, the \"simple\" table name)\n\n"
              + "Note that the expression can only test against 'operations', if 'fetch' is set to 'ALL'.\n\n"
              + "Hint: when filtering commits, you can determine whether commits are \"missing\" (filtered) by checking whether 'LogEntry.parentCommitHash' is different from the hash of the previous commit in the log response.",
      examples = {
        @ExampleObject(ref = "expr_by_commit_author"),
        @ExampleObject(ref = "expr_by_commit_committer"),
        @ExampleObject(ref = "expr_by_commitTime"),
        @ExampleObject(ref = "expr_by_commit_operations_table_name"),
        @ExampleObject(ref = "expr_by_commit_operations_type")
      })
  @QueryParam("filter")
  private String filter;

  @Parameter(
      description =
          "Specify how much information to be returned. Will fetch additional metadata such as parent commit hash and operations in a commit, for each commit if set to 'ALL'.")
  @QueryParam("fetch")
  @Nullable
  private FetchOption fetchOption;

  public CommitLogParams() {}

  @org.immutables.builder.Builder.Constructor
  CommitLogParams(
      @Nullable String startHash,
      @Nullable String endHash,
      @Nullable Integer maxRecords,
      @Nullable String pageToken,
      @Nullable String filter,
      @Nullable FetchOption fetchOption) {
    super(maxRecords, pageToken);
    this.startHash = startHash;
    this.endHash = endHash;
    this.filter = filter;
    this.fetchOption = fetchOption;
  }

  @Nullable
  public String startHash() {
    return startHash;
  }

  @Nullable
  public String endHash() {
    return endHash;
  }

  @Nullable
  public String filter() {
    return filter;
  }

  @Nullable
  public FetchOption fetchOption() {
    return fetchOption;
  }

  public static CommitLogParamsBuilder builder() {
    return new CommitLogParamsBuilder();
  }

  public static CommitLogParams empty() {
    return builder().build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CommitLogParams.class.getSimpleName() + "[", "]")
        .add("startHash='" + startHash + "'")
        .add("endHash='" + endHash + "'")
        .add("maxRecords=" + maxRecords())
        .add("pageToken='" + pageToken() + "'")
        .add("filter='" + filter + "'")
        .add("fetchOption='" + fetchOption + "'")
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CommitLogParams that = (CommitLogParams) o;
    return Objects.equals(startHash, that.startHash)
        && Objects.equals(endHash, that.endHash)
        && Objects.equals(maxRecords(), that.maxRecords())
        && Objects.equals(pageToken(), that.pageToken())
        && Objects.equals(filter, that.filter)
        && Objects.equals(fetchOption, that.fetchOption);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startHash, endHash, maxRecords(), pageToken(), filter, fetchOption);
  }
}
