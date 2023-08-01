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

import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX;

import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.model.FetchOption;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@code
 * HttpTreeApi#getCommitLog(String, CommitLogParams)}.
 *
 * <p>For easier usage of this class, there is {@link CommitLogParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class CommitLogParams extends AbstractParams<CommitLogParams> {

  @Nullable
  @jakarta.annotation.Nullable
  @Pattern(
      regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
      message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
      message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
  @Parameter(
      description =
          "Hash on the given ref to identify the commit where the operation of fetching the log "
              + "should stop, i.e. the 'far' end of the commit log, returned late in the result.",
      examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
  @QueryParam("limit-hash")
  @jakarta.ws.rs.QueryParam("limit-hash")
  private String startHash;

  @Nullable
  @jakarta.annotation.Nullable
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
  @jakarta.ws.rs.QueryParam("filter")
  private String filter;

  @Parameter(
      description =
          "Specify how much information to be returned. Will fetch additional metadata such as parent commit hash and operations in a commit, for each commit if set to 'ALL'.")
  @QueryParam("fetch")
  @jakarta.ws.rs.QueryParam("fetch")
  @Nullable
  @jakarta.annotation.Nullable
  private FetchOption fetchOption;

  public CommitLogParams() {}

  @Constructor
  CommitLogParams(
      @Nullable @jakarta.annotation.Nullable String startHash,
      @Nullable @jakarta.annotation.Nullable Integer maxRecords,
      @Nullable @jakarta.annotation.Nullable String pageToken,
      @Nullable @jakarta.annotation.Nullable String filter,
      @Nullable @jakarta.annotation.Nullable FetchOption fetchOption) {
    super(maxRecords, pageToken);
    this.startHash = startHash;
    this.filter = filter;
    this.fetchOption = fetchOption;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public String startHash() {
    return startHash;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public String filter() {
    return filter;
  }

  @Nullable
  @jakarta.annotation.Nullable
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
  public CommitLogParams forNextPage(String pageToken) {
    return new CommitLogParams(startHash, maxRecords(), pageToken, filter, fetchOption);
  }
}
