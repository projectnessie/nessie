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
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

public class GetReferenceParams {

  @Parameter(
      description = "name of ref to fetch",
      examples = {@ExampleObject(ref = "ref")})
  @PathParam("ref")
  @NotNull
  @Pattern(
      regexp = Validation.REF_NAME_OR_HASH_REGEX,
      message = Validation.REF_NAME_OR_HASH_MESSAGE)
  private String refName;

  @Parameter(
      description =
          "If set to true, will fetch additional metadata for references.\n\n"
              + "A returned Branch instance will have the following information:\n\n"
              + "- numCommitsAhead (number of commits ahead of the default branch)\n\n"
              + "- numCommitsBehind (number of commits behind the default branch)\n\n"
              + "- commitMetaOfHEAD (the commit metadata of the HEAD commit)\n\n"
              + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
              + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
              + "- numTotalCommits (the total number of commits in this reference).\n\n"
              + "A returned Tag instance will only contain the 'commitMetaOfHEAD' and 'numTotalCommits' fields.\n\n"
              + "Note that computing & fetching additional metadata might be computationally expensive on the server-side, so this flag should be used with care.")
  @QueryParam("fetchAdditionalInfo")
  private boolean fetchAdditionalInfo;

  public GetReferenceParams() {}

  private GetReferenceParams(String refName, boolean fetchAdditionalInfo) {
    this.refName = refName;
    this.fetchAdditionalInfo = fetchAdditionalInfo;
  }

  private GetReferenceParams(Builder builder) {
    this(builder.refName, builder.fetchAdditionalInfo);
  }

  public boolean isFetchAdditionalInfo() {
    return fetchAdditionalInfo;
  }

  public String getRefName() {
    return refName;
  }

  public static GetReferenceParams.Builder builder() {
    return new GetReferenceParams.Builder();
  }

  public static GetReferenceParams empty() {
    return new GetReferenceParams.Builder().build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetReferenceParams that = (GetReferenceParams) o;
    return fetchAdditionalInfo == that.fetchAdditionalInfo && Objects.equals(refName, that.refName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(refName, fetchAdditionalInfo);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GetReferenceParams.class.getSimpleName() + "[", "]")
        .add("refName='" + refName + "'")
        .add("fetchAdditionalInfo=" + fetchAdditionalInfo)
        .toString();
  }

  public static class Builder {
    private String refName;
    private boolean fetchAdditionalInfo;

    private Builder() {}

    public Builder refName(String refName) {
      this.refName = refName;
      return this;
    }

    public Builder fetchAdditionalInfo(boolean fetchAdditionalInfo) {
      this.fetchAdditionalInfo = fetchAdditionalInfo;
      return this;
    }

    private void validate() {}

    public GetReferenceParams build() {
      validate();
      return new GetReferenceParams(this);
    }
  }
}
