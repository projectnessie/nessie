/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.formats.iceberg.meta;

import static org.projectnessie.catalog.formats.iceberg.manifest.Avro.makeCompatibleName;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergNestedField.class)
@JsonDeserialize(as = ImmutableIcebergNestedField.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergNestedField {

  static Builder builder() {
    return ImmutableIcebergNestedField.builder();
  }

  static IcebergNestedField nestedField(
      int id, String name, boolean required, IcebergType type, String doc) {
    return ImmutableIcebergNestedField.of(id, name, required, type, doc);
  }

  int id();

  String name();

  boolean required();

  IcebergType type();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String doc();

  @Value.Lazy
  @JsonIgnore
  default String avroName() {
    return makeCompatibleName(name());
  }

  @Value.NonAttribute
  @Value.Lazy
  @JsonIgnore
  default Schema avroSchema() {
    return type().avroSchema(id());
  }

  IcebergNestedField withRequired(boolean required);

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergNestedField field);

    @CanIgnoreReturnValue
    Builder id(int id);

    @CanIgnoreReturnValue
    Builder name(String name);

    @CanIgnoreReturnValue
    Builder required(boolean required);

    @CanIgnoreReturnValue
    Builder type(IcebergType type);

    @CanIgnoreReturnValue
    Builder doc(String doc);

    IcebergNestedField build();
  }
}
