/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergS3SignResponse.class)
@JsonDeserialize(as = ImmutableIcebergS3SignResponse.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergS3SignResponse {
  String uri();

  Map<String, List<String>> headers();

  static Builder builder() {
    return ImmutableIcebergS3SignResponse.builder();
  }

  static IcebergS3SignResponse icebergS3SignResponse(
      String uri, Map<String, List<String>> headers) {
    return ImmutableIcebergS3SignResponse.of(uri, headers);
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergS3SignResponse instance);

    @CanIgnoreReturnValue
    Builder uri(String uri);

    @CanIgnoreReturnValue
    Builder putHeader(String key, List<String> value);

    @CanIgnoreReturnValue
    Builder putHeader(Map.Entry<String, ? extends List<String>> entry);

    @CanIgnoreReturnValue
    Builder headers(Map<String, ? extends List<String>> entries);

    @CanIgnoreReturnValue
    Builder putAllHeaders(Map<String, ? extends List<String>> entries);

    IcebergS3SignResponse build();
  }
}
