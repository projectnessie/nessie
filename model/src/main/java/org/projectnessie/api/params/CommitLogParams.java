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

import java.util.StringJoiner;
import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@link
 * org.projectnessie.api.TreeApi#getCommitLog(String, CommitLogParams)}.
 *
 * <p>For easier usage of this class, there is {@link CommitLogParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class CommitLogParams {

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description = "a particular hash on the given ref",
      examples = {@ExampleObject(ref = "hash")})
  @QueryParam("hashOnRef")
  private String hashOnRef;

  @Parameter(
      description = "maximum number of commit-log entries to return, just a hint for the server")
  @QueryParam("max")
  private Integer maxRecords;

  @Parameter(
      description = "pagination continuation token, as returned in the previous LogResponse.token")
  @QueryParam("pageToken")
  private String pageToken;

  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
              + "Usable variables within the expression are 'commit.author' (string) / 'commit.committer' (string) / 'commit.commitTime' (timestamp) / 'commit.hash' (string) / 'commit.message' (string) / 'commit.properties' (map)",
      examples = {
        @ExampleObject(ref = "expr_by_commit_author"),
        @ExampleObject(ref = "expr_by_commit_committer"),
        @ExampleObject(ref = "expr_by_commitTime")
      })
  @QueryParam("query_expression")
  private String queryExpression;

  public CommitLogParams() {}

  private CommitLogParams(
      String hashOnRef, Integer maxRecords, String pageToken, String queryExpression) {
    this.hashOnRef = hashOnRef;
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
    this.queryExpression = queryExpression;
  }

  private CommitLogParams(Builder builder) {
    this(builder.hashOnRef, builder.maxRecords, builder.pageToken, builder.queryExpression);
  }

  @Nullable
  public String hashOnRef() {
    return hashOnRef;
  }

  public Integer maxRecords() {
    return maxRecords;
  }

  public String pageToken() {
    return pageToken;
  }

  public String queryExpression() {
    return queryExpression;
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
        .add("hashOnRef='" + hashOnRef + "'")
        .add("maxRecords=" + maxRecords)
        .add("pageToken='" + pageToken + "'")
        .add("queryExpression='" + queryExpression + "'")
        .toString();
  }

  public static class Builder {

    private String hashOnRef;
    private Integer maxRecords;
    private String pageToken;
    private String queryExpression;

    private Builder() {}

    public Builder hashOnRef(String hashOnRef) {
      this.hashOnRef = hashOnRef;
      return this;
    }

    public Builder maxRecords(Integer maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public Builder pageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    public Builder expression(String queryExpression) {
      this.queryExpression = queryExpression;
      return this;
    }

    public Builder from(CommitLogParams params) {
      return hashOnRef(params.hashOnRef)
          .maxRecords(params.maxRecords)
          .pageToken(params.pageToken)
          .expression(params.queryExpression);
    }

    private void validate() {}

    public CommitLogParams build() {
      validate();
      return new CommitLogParams(this);
    }
  }
}
