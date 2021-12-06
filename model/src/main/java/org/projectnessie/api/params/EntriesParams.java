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
public class EntriesParams extends AbstractParams {

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description = "a particular hash on the given ref",
      examples = {@ExampleObject(ref = "nullHash"), @ExampleObject(ref = "hash")})
  @QueryParam("hashOnRef")
  private String hashOnRef;

  @Nullable
  @Parameter(
      description =
          "If set > 0 will filter the results to only return namespaces/tables to the depth of namespaceDepth. If not set or <=0 has no effect\n"
              + "Setting this parameter > 0 will turn off paging.")
  @QueryParam("namespaceDepth")
  private Integer namespaceDepth;

  @Nullable
  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
              + "Usable variables within the expression are 'entry.namespace' (string) & 'entry.contentType' (string)",
      examples = {
        @ExampleObject(ref = "expr_by_namespace"),
        @ExampleObject(ref = "expr_by_contentType"),
        @ExampleObject(ref = "expr_by_namespace_and_contentType")
      })
  @QueryParam("filter")
  private String filter;

  public EntriesParams() {}

  private EntriesParams(
      String hashOnRef,
      Integer maxRecords,
      String pageToken,
      Integer namespaceDepth,
      String filter) {
    super(maxRecords, pageToken);
    this.hashOnRef = hashOnRef;
    this.namespaceDepth = namespaceDepth;
    this.filter = filter;
  }

  private EntriesParams(Builder builder) {
    this(
        builder.hashOnRef,
        builder.maxRecords,
        builder.pageToken,
        builder.namespaceDepth,
        builder.filter);
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

  public String filter() {
    return filter;
  }

  public Integer namespaceDepth() {
    return namespaceDepth;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", EntriesParams.class.getSimpleName() + "[", "]")
        .add("hashOnRef='" + hashOnRef + "'")
        .add("maxRecords=" + maxRecords())
        .add("pageToken='" + pageToken() + "'")
        .add("filter='" + filter + "'")
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
        && Objects.equals(maxRecords(), that.maxRecords())
        && Objects.equals(pageToken(), that.pageToken())
        && Objects.equals(namespaceDepth, that.namespaceDepth)
        && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hashOnRef, maxRecords(), pageToken(), namespaceDepth, filter);
  }

  public static class Builder extends AbstractParams.Builder<Builder> {

    private String hashOnRef;
    private String filter;
    private Integer namespaceDepth;

    private Builder() {}

    public EntriesParams.Builder hashOnRef(String hashOnRef) {
      this.hashOnRef = hashOnRef;
      return this;
    }

    public EntriesParams.Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    public Builder namespaceDepth(Integer namespaceDepth) {
      this.namespaceDepth = namespaceDepth;
      return this;
    }

    public EntriesParams.Builder from(EntriesParams params) {
      return hashOnRef(params.hashOnRef)
          .maxRecords(params.maxRecords())
          .pageToken(params.pageToken())
          .namespaceDepth(params.namespaceDepth)
          .filter(params.filter);
    }

    private void validate() {}

    public EntriesParams build() {
      validate();
      return new EntriesParams(this);
    }
  }
}
