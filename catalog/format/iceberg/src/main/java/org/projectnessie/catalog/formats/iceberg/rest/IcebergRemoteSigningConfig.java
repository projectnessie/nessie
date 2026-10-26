/*
 * Copyright (C) 2026 Dremio
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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergRemoteSigningConfig.class)
@tools.jackson.databind.annotation.JsonSerialize(as = ImmutableIcebergRemoteSigningConfig.class)
@JsonDeserialize(as = ImmutableIcebergRemoteSigningConfig.class)
@tools.jackson.databind.annotation.JsonDeserialize(as = ImmutableIcebergRemoteSigningConfig.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@tools.jackson.databind.annotation.JsonNaming(
    tools.jackson.databind.PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergRemoteSigningConfig {
  /** Static properties clients must forward in every signing request. */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> properties();

  /** Static headers clients must forward to the signing endpoint. */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, List<String>> headers();
}
