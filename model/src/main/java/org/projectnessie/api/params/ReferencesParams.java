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

/**
 * The purpose of this class is to include optional parameters that can be passed to {@code
 * HttpTreeApi#getEntries(String, EntriesParams)}.
 *
 * <p>For easier usage of this class, there is {@link ReferencesParams#builder()}, which allows
 * configuring/setting the different parameters.
 */
public class ReferencesParams extends AbstractParams {

  public ReferencesParams() {}

  private ReferencesParams(Integer maxRecords, String pageToken) {
    super(maxRecords, pageToken);
  }

  private ReferencesParams(Builder builder) {
    this(builder.maxRecords, builder.pageToken);
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
        && Objects.equals(pageToken(), that.pageToken());
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxRecords(), pageToken());
  }

  public static class Builder extends AbstractParams.Builder<Builder> {

    private Builder() {}

    public ReferencesParams.Builder from(ReferencesParams params) {
      return maxRecords(params.maxRecords()).pageToken(params.pageToken());
    }

    private void validate() {}

    public ReferencesParams build() {
      validate();
      return new ReferencesParams(this);
    }
  }
}
