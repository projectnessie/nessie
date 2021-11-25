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
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.api.http.HttpTreeApi;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@link
 * HttpTreeApi#getAllReferences(ReferencesParams)}
 *
 * <p>For easier usage of this class, there is {@link ReferencesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class ReferencesParams extends AbstractParams {

  public ReferencesParams() {}

  @Parameter(
      description =
          "If set to true, will fetch additional metadata for branches, such as number of commits ahead/behind or the common ancestor in relation to the default branch, and the commit metadata for the HEAD commit.\n\n"
              + "A returned Branch instance will then have a map of 'metadataProperties' with the following keys:\n\n"
              + "- numCommitsAhead (number of commits ahead of the default branch)\n\n"
              + "- numCommitsBehind (number of commits behind the default branch)\n\n"
              + "- headCommitMeta (the commit metadata of the HEAD commit)\n\n"
              + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
              + "Note that computing & fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care.")
  @QueryParam("fetchAdditionalInfo")
  private boolean fetchAdditionalInfo;

  public boolean isFetchAdditionalInfo() {
    return fetchAdditionalInfo;
  }

  private ReferencesParams(Integer maxRecords, String pageToken, boolean fetchAdditionalInfo) {
    super(maxRecords, pageToken);
    this.fetchAdditionalInfo = fetchAdditionalInfo;
  }

  private ReferencesParams(Builder builder) {
    this(builder.maxRecords, builder.pageToken, builder.fetchAdditionalInfo);
  }

  public static ReferencesParams.Builder builder() {
    return new ReferencesParams.Builder();
  }

  public static ReferencesParams empty() {
    return new ReferencesParams.Builder().build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ReferencesParams.class.getSimpleName() + "[", "]")
        .add("maxRecords=" + maxRecords())
        .add("pageToken='" + pageToken() + "'")
        .add("fetchAdditionalInfo=" + fetchAdditionalInfo)
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
    ReferencesParams that = (ReferencesParams) o;
    return Objects.equals(maxRecords(), that.maxRecords())
        && Objects.equals(pageToken(), that.pageToken())
        && Objects.equals(fetchAdditionalInfo, that.fetchAdditionalInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxRecords(), pageToken(), fetchAdditionalInfo);
  }

  public static class Builder extends AbstractParams.Builder<Builder> {

    private Builder() {}

    private boolean fetchAdditionalInfo = false;

    public ReferencesParams.Builder from(ReferencesParams params) {
      return maxRecords(params.maxRecords())
          .pageToken(params.pageToken())
          .fetchAdditionalInfo(params.fetchAdditionalInfo);
    }

    public Builder fetchAdditionalInfo(boolean fetchAdditionalInfo) {
      this.fetchAdditionalInfo = fetchAdditionalInfo;
      return this;
    }

    private void validate() {}

    public ReferencesParams build() {
      validate();
      return new ReferencesParams(this);
    }
  }
}
