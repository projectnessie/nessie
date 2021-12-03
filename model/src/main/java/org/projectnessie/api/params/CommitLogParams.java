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

  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n\n"
              + "Usable variables within the expression are:\n\n"
              + "- 'commit' with fields 'author' (string), 'committer' (string), 'commitTime' (timestamp), 'hash' (string), ',message' (string), 'properties' (map)\n\n"
              + "- 'operations' (list), each operation has the fields 'type' (string, either 'PUT' or 'DELETE'), 'key' (string, namespace + table name), 'keyElements' (list of strings), 'namespace' (string), 'namespaceElements' (list of strings) and 'name' (string, the \"simple\" table name)\n\n"
              + "Note that the expression can only test against 'operations', if 'fetchAdditionalInfo' is true.\n\n"
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
          "If set to true, will fetch additional metadata, parent commit hash and operations in a commit, for each commit.")
  @QueryParam("fetchAdditionalInfo")
  private boolean fetchAdditionalInfo;

  public boolean isFetchAdditionalInfo() {
    return fetchAdditionalInfo;
  }

  public CommitLogParams() {}

  private CommitLogParams(
      String startHash,
      String endHash,
      Integer maxRecords,
      String pageToken,
      String filter,
      boolean fetchAdditionalInfo) {
    super(maxRecords, pageToken);
    this.startHash = startHash;
    this.endHash = endHash;
    this.filter = filter;
    this.fetchAdditionalInfo = fetchAdditionalInfo;
  }

  private CommitLogParams(Builder builder) {
    this(
        builder.startHash,
        builder.endHash,
        builder.maxRecords,
        builder.pageToken,
        builder.filter,
        builder.fetchAdditionalInfo);
  }

  @Nullable
  public String startHash() {
    return startHash;
  }

  @Nullable
  public String endHash() {
    return endHash;
  }

  public String filter() {
    return filter;
  }

  public static CommitLogParams.Builder builder() {
    return new CommitLogParams.Builder();
  }

  public static CommitLogParams empty() {
    return new CommitLogParams.Builder().build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CommitLogParams.class.getSimpleName() + "[", "]")
        .add("startHash='" + startHash + "'")
        .add("endHash='" + endHash + "'")
        .add("maxRecords=" + maxRecords())
        .add("pageToken='" + pageToken() + "'")
        .add("filter='" + filter + "'")
        .add("fetchAdditionalInfo='" + fetchAdditionalInfo + "'")
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
        && Objects.equals(fetchAdditionalInfo, that.fetchAdditionalInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startHash, endHash, maxRecords(), pageToken(), filter, fetchAdditionalInfo);
  }

  public static class Builder extends AbstractParams.Builder<Builder> {

    private String startHash;
    private String endHash;
    private String filter;
    private boolean fetchAdditionalInfo;

    private Builder() {}

    public Builder startHash(String startHash) {
      this.startHash = startHash;
      return this;
    }

    public Builder endHash(String endHash) {
      this.endHash = endHash;
      return this;
    }

    public Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder fetchAdditionalInfo(boolean fetchAdditionalInfo) {
      this.fetchAdditionalInfo = fetchAdditionalInfo;
      return this;
    }

    public Builder from(CommitLogParams params) {
      return startHash(params.startHash)
          .endHash(params.endHash)
          .maxRecords(params.maxRecords())
          .pageToken(params.pageToken())
          .filter(params.filter)
          .fetchAdditionalInfo(params.fetchAdditionalInfo);
    }

    private void validate() {}

    public CommitLogParams build() {
      validate();
      return new CommitLogParams(this);
    }
  }
}
