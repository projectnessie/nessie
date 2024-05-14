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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableUDF.class)
@JsonDeserialize(as = ImmutableUDF.class)
@JsonTypeName("UDF")
public abstract class UDF extends Content {

  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public abstract String getSqlText();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public abstract String getDialect();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public abstract Long getVersionId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public abstract String getMetadataLocation();

  @Override
  public Type getType() {
    return Type.UDF;
  }

  @Override
  public abstract UDF withId(String id);

  public static ImmutableUDF.Builder builder() {
    return ImmutableUDF.builder();
  }

  public static UDF of(String metadataLocation, long versionId) {
    return builder().metadataLocation(metadataLocation).versionId(versionId).build();
  }

  public static UDF of(String id, String metadataLocation, long versionId) {
    return builder().id(id).metadataLocation(metadataLocation).versionId(versionId).build();
  }

  @Deprecated
  public static UDF of(String dialect, String sqlText) {
    return builder().dialect(dialect).sqlText(sqlText).build();
  }

  @Deprecated
  public static UDF of(String id, String dialect, String sqlText) {
    return builder().id(id).dialect(dialect).sqlText(sqlText).build();
  }
}
