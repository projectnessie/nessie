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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@link
 * org.projectnessie.api.TreeApi#getEntries(String, EntriesParams)}.
 *
 * <p>For easier usage of this class, there is {@link EntriesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class EntriesParams {

  @Parameter(description = "maximum number of entries to return, just a hint for the server")
  @QueryParam("max")
  private Integer maxRecords;

  @Parameter(
      description =
          "pagination continuation token, as returned in the previous EntriesResponse.token")
  @QueryParam("pageToken")
  private String pageToken;

  @Parameter(
      description = "list of value types to return. Return all if empty",
      examples = {@ExampleObject(ref = "types")})
  @QueryParam("types")
  private List<String> types;

  @Parameter(
      description =
          "The namespace to filter by. For a given table name 'a.b.c.tableName', the namespace is 'a.b.c', thus a valid"
              + "namespace to filter by would be 'a' / 'a.b' / 'a.b.c'. Setting the namespace filter to 'a.b.c.tableName' would not return any"
              + "results, because 'tableName' is not part of the namespace."
              + "No filtering will happen if this is set to null/empty",
      examples = {@ExampleObject(ref = "namespace")})
  @QueryParam("namespace")
  private String namespace;

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
      Integer maxRecords,
      String pageToken,
      List<String> types,
      String namespace,
      String queryExpression) {
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
    this.types = types;
    this.namespace = namespace;
    this.queryExpression = queryExpression;
  }

  private EntriesParams(Builder builder) {
    this(
        builder.maxRecords,
        builder.pageToken,
        builder.types,
        builder.namespace,
        builder.queryExpression);
  }

  public static EntriesParams.Builder builder() {
    return new EntriesParams.Builder();
  }

  public static EntriesParams empty() {
    return new EntriesParams.Builder().build();
  }

  public Integer getMaxRecords() {
    return maxRecords;
  }

  public String getPageToken() {
    return pageToken;
  }

  public List<String> getTypes() {
    return types;
  }

  public String getQueryExpression() {
    return queryExpression;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EntriesParams.class.getSimpleName() + "[", "]")
        .add("maxRecords=" + maxRecords)
        .add("pageToken='" + pageToken + "'")
        .add("types=" + types)
        .add("namespace='" + namespace + "'")
        .add("queryExpression='" + queryExpression + "'")
        .toString();
  }

  public static class Builder {

    private Integer maxRecords;
    private String pageToken;
    private String namespace;
    private List<String> types = Collections.emptyList();
    private String queryExpression;

    private Builder() {}

    public EntriesParams.Builder maxRecords(Integer maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public EntriesParams.Builder pageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    public EntriesParams.Builder types(List<String> types) {
      this.types = types;
      return this;
    }

    public EntriesParams.Builder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public EntriesParams.Builder queryExpression(String queryExpression) {
      this.queryExpression = queryExpression;
      return this;
    }

    public EntriesParams.Builder from(EntriesParams params) {
      return maxRecords(params.maxRecords)
          .pageToken(params.pageToken)
          .namespace(params.namespace)
          .types(params.types)
          .queryExpression(params.queryExpression);
    }

    private void validate() {
      Objects.requireNonNull(types, "types must be non-null");
    }

    public EntriesParams build() {
      validate();
      return new EntriesParams(this);
    }
  }
}
