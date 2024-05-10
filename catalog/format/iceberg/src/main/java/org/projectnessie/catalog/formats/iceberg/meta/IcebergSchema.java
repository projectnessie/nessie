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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import org.apache.avro.Schema;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergSchema.class)
@JsonDeserialize(as = ImmutableIcebergSchema.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergSchema {
  int INITIAL_SCHEMA_ID = 0;
  int INITIAL_COLUMN_ID = 0;

  static Builder builder() {
    return ImmutableIcebergSchema.builder();
  }

  static IcebergSchema schema(
      int schemaId, List<Integer> identifierFieldIds, List<IcebergNestedField> fields) {
    return ImmutableIcebergSchema.of("struct", schemaId, identifierFieldIds, fields);
  }

  @Value.Default
  default String type() {
    return "struct";
  }

  @Value.Default
  default int schemaId() {
    return 0;
  }

  IcebergSchema withSchemaId(int schemaId);

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<Integer> identifierFieldIds();

  List<IcebergNestedField> fields();

  // TODO move this elsewhere and memoize the constructed Avro-Schemas, at least temporarily, which
  //  is useful when generating many manifest-files. See IcebergManifestFileWriter &
  //  IcebergManifestListWriter
  @JsonIgnore
  default Schema avroSchema(String recordName) {
    // TODO Verify field-id, field generation, etc
    //  see org.apache.iceberg.avro.AvroSchemaUtil.convert(org.apache.avro.Schema)
    // TODO "r102" is the partition-spec record-name - need to use the table name
    return IcebergType.structType(fields(), recordName).avroSchema(1);
  }

  default boolean sameSchema(IcebergSchema anotherSchema) {
    return fields().equals(anotherSchema.fields())
        // TODO is the above the same as
        //   asStruct().equals(anotherSchema.asStruct())
        //   ??
        && identifierFieldIds().equals(anotherSchema.identifierFieldIds());
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergSchema schema);

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder type(String type);

    @CanIgnoreReturnValue
    Builder schemaId(int schemaId);

    @CanIgnoreReturnValue
    Builder addIdentifierFieldId(int element);

    @CanIgnoreReturnValue
    Builder addIdentifierFieldIds(int... elements);

    @CanIgnoreReturnValue
    Builder identifierFieldIds(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addAllIdentifierFieldIds(Iterable<Integer> elements);

    @CanIgnoreReturnValue
    Builder addField(IcebergNestedField element);

    @CanIgnoreReturnValue
    Builder addFields(IcebergNestedField... elements);

    @CanIgnoreReturnValue
    Builder fields(Iterable<? extends IcebergNestedField> elements);

    @CanIgnoreReturnValue
    Builder addAllFields(Iterable<? extends IcebergNestedField> elements);

    IcebergSchema build();
  }
}
