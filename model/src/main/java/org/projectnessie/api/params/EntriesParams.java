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
 * HttpTreeApi#getEntries(String, EntriesParams)}.
 *
 * <p>For easier usage of this class, there is {@link EntriesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class EntriesParams {

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description = "a particular hash on the given ref",
      examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
  @QueryParam("hashOnRef")
  private String hashOnRef;

  @Parameter(description = "maximum number of entries to return, just a hint for the server")
  @QueryParam("max")
  private Integer maxRecords;

  @Parameter(
      description =
          "pagination continuation token, as returned in the previous EntriesResponse.token")
  @QueryParam("pageToken")
  private String pageToken;

  @Parameter(
      description =
          "If set > 0 will filter the results to only return namespaces/tables to the depth of namespaceDepth. If not set or <=0 has no effect\n"
              + "Setting this paramter > 0 will turn off paging.")
  @QueryParam("namespaceDepth")
  private Integer namespaceDepth;

  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
              + "Usable variables within the expression are 'entry.namespace' (string) & 'entry.contentType' (string)",
      examples = {
        @ExampleObject(ref = "expr_by_namespace"),
        @ExampleObject(ref = "expr_by_contentType"),
        @ExampleObject(ref = "expr_by_namespace_and_contentType")
      })
  @QueryParam("query_expression")
  private String queryExpression;

  public EntriesParams() {}

  private EntriesParams(
      String hashOnRef,
      Integer maxRecords,
      String pageToken,
      Integer namespaceDepth,
      String queryExpression) {
    this.hashOnRef = hashOnRef;
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
    this.namespaceDepth = namespaceDepth;
    this.queryExpression = queryExpression;
  }

  private EntriesParams(Builder builder) {
    this(
        builder.hashOnRef,
        builder.maxRecords,
        builder.pageToken,
        builder.namespaceDepth,
        builder.queryExpression);
  }

  public static EntriesParams.Builder builder() {
    return new EntriesParams.Builder();
  }

  public static EntriesParams empty() {
    return new EntriesParams.Builder().build();
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

  public Integer namespaceDepth() {
    return namespaceDepth;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EntriesParams.class.getSimpleName() + "[", "]")
        .add("hashOnRef='" + hashOnRef + "'")
        .add("maxRecords=" + maxRecords)
        .add("pageToken='" + pageToken + "'")
        .add("queryExpression='" + queryExpression + "'")
        .add("namespaceDepth='" + namespaceDepth + "'")
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
    EntriesParams that = (EntriesParams) o;
    return Objects.equals(hashOnRef, that.hashOnRef)
        && Objects.equals(maxRecords, that.maxRecords)
        && Objects.equals(pageToken, that.pageToken)
        && Objects.equals(namespaceDepth, that.namespaceDepth)
        && Objects.equals(queryExpression, that.queryExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hashOnRef, maxRecords, pageToken, namespaceDepth, queryExpression);
  }

  public static class Builder {

    private String hashOnRef;
    private Integer maxRecords;
    private String pageToken;
    private String queryExpression;
    private Integer namespaceDepth;

    private Builder() {}

    public EntriesParams.Builder hashOnRef(String hashOnRef) {
      this.hashOnRef = hashOnRef;
      return this;
    }

    public EntriesParams.Builder maxRecords(Integer maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public EntriesParams.Builder pageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    public EntriesParams.Builder expression(String queryExpression) {
      this.queryExpression = queryExpression;
      return this;
    }

    public Builder namespaceDepth(Integer namespaceDepth) {
      this.namespaceDepth = namespaceDepth;
      return this;
    }

    public EntriesParams.Builder from(EntriesParams params) {
      return hashOnRef(params.hashOnRef)
          .maxRecords(params.maxRecords)
          .pageToken(params.pageToken)
          .namespaceDepth(params.namespaceDepth)
          .expression(params.queryExpression);
    }

    private void validate() {}

    public EntriesParams build() {
      validate();
      return new EntriesParams(this);
    }
  }
}
