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
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

/**
 * The purpose of this class is to include optional parameters that can be passed to {@code
 * HttpRefLogApi#getRefLog(RefLogParams)}.
 *
 * <p>For easier usage of this class, there is {@link RefLogParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class RefLogParams extends AbstractParams {

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description =
          "Hash of the reflog (inclusive) to start from (in chronological sense), the 'far' end of the reflog, "
              + "returned "
              + "'late' in the result.")
  @QueryParam("startHash")
  private String startHash;

  @Nullable
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description =
          "Hash of the reflog (inclusive) to end at (in chronological sense), the 'near' end of the reflog, returned "
              + "'early' "
              + "in the result.")
  @QueryParam("endHash")
  private String endHash;

  public RefLogParams() {}

  private RefLogParams(String startHash, String endHash, Integer maxRecords, String pageToken) {
    super(maxRecords, pageToken);
    this.startHash = startHash;
    this.endHash = endHash;
  }

  private RefLogParams(Builder builder) {
    this(builder.startHash, builder.endHash, builder.maxRecords, builder.pageToken);
  }

  @Nullable
  public String startHash() {
    return startHash;
  }

  @Nullable
  public String endHash() {
    return endHash;
  }

  public static RefLogParams.Builder builder() {
    return new RefLogParams.Builder();
  }

  public static RefLogParams empty() {
    return new RefLogParams.Builder().build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RefLogParams.class.getSimpleName() + "[", "]")
        .add("startHash='" + startHash + "'")
        .add("endHash='" + endHash + "'")
        .add("maxRecords=" + maxRecords())
        .add("pageToken='" + pageToken() + "'")
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
    RefLogParams that = (RefLogParams) o;
    return Objects.equals(startHash, that.startHash)
        && Objects.equals(endHash, that.endHash)
        && Objects.equals(maxRecords(), that.maxRecords())
        && Objects.equals(pageToken(), that.pageToken());
  }

  @Override
  public int hashCode() {
    return Objects.hash(startHash, endHash, maxRecords(), pageToken());
  }

  public static class Builder extends AbstractParams.Builder<Builder> {

    private String startHash;
    private String endHash;

    private Builder() {}

    public Builder startHash(String startHash) {
      this.startHash = startHash;
      return this;
    }

    public Builder endHash(String endHash) {
      this.endHash = endHash;
      return this;
    }

    public Builder from(RefLogParams params) {
      return startHash(params.startHash)
          .endHash(params.endHash)
          .maxRecords(params.maxRecords())
          .pageToken(params.pageToken());
    }

    public RefLogParams build() {
      return new RefLogParams(this);
    }
  }
}
