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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.structType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.projectnessie.catalog.formats.iceberg.types.IcebergStructType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergPartitionSpec.class)
@JsonDeserialize(as = ImmutableIcebergPartitionSpec.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergPartitionSpec {
  int INITIAL_SPEC_ID = 0;
  int MIN_PARTITION_ID = 1000;

  static Builder builder() {
    return ImmutableIcebergPartitionSpec.builder();
  }

  static IcebergPartitionSpec partitionSpec(int specId, List<IcebergPartitionField> fields) {
    return ImmutableIcebergPartitionSpec.of(specId, fields);
  }

  IcebergPartitionSpec UNPARTITIONED_SPEC = partitionSpec(0, emptyList());

  static IcebergPartitionSpec unpartitioned() {
    return UNPARTITIONED_SPEC;
  }

  int specId();

  IcebergPartitionSpec withSpecId(int specId);

  List<IcebergPartitionField> fields();

  // TODO move this elsewhere and memoize the constructed Avro-Schemas, at least temporarily, which
  //  is useful when generating many manifest-files. See IcebergManifestFileWriter &
  //  IcebergManifestListWriter
  default Schema avroSchema(IcebergSchema schema, String recordName) {
    List<IcebergNestedField> partitionFields = new ArrayList<>(fields().size());
    for (IcebergPartitionField partitionField : fields()) {
      IcebergNestedField schemaField = null;
      int sourceId = partitionField.sourceId();
      for (IcebergNestedField field : schema.fields()) {
        if (field.id() == sourceId) {
          schemaField = field;
          break;
        }
      }

      checkState(schemaField != null, "No field with ID %s in schema", sourceId);

      // TODO need ability to find fields in schema (nested?) by field id
      // TODO since partition field IDs are assigned starting at 1000, those could collide with
      //  column-IDs
      //  - can there be duplicates?
      //  - what happens if there is a collision?
      // TODO does iceberg allow these:
      //  - dropping a column that's used in a partition-spec or sort-order?
      //  - changing the type of a column that's used in a partition-spec or sort-order?
      //  - removing a field from a struct (nested field) that's used in a partition-spec or
      //    sort-order?
      //  - changing the type of a a field from a struct (nested field) that's used in a
      //    partition-spec or sort-order?
      //  - is it possible to chain a partition fields?
      //    for example:
      //      PartitionField(source-id=3, field-id=4, transform=...) -->
      //      PartitionField(source-id=2, field-id=3, transform=...) -->
      //      PartitionField(source-id=1, field-id=2, transform=...)

      // TODO use transformed type
      IcebergType partitionFieldType = partitionField.type(schema);

      IcebergNestedField asNestedField =
          nestedField(
              partitionField.fieldId(), partitionField.name(), false, partitionFieldType, null);
      partitionFields.add(asNestedField);
    }

    IcebergStructType struct = structType(partitionFields, recordName);
    return struct.avroSchema(102);
  }

  default boolean compatibleWith(IcebergPartitionSpec other) {
    if (equals(other)) {
      return true;
    }

    if (fields().size() != other.fields().size()) {
      return false;
    }

    for (int i = 0; i < fields().size(); i += 1) {
      IcebergPartitionField thisField = fields().get(i);
      IcebergPartitionField thatField = other.fields().get(i);
      if (thisField.sourceId() != thatField.sourceId()
          || !thisField.transform().equals(thatField.transform())
          || !thisField.name().equals(thatField.name())) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergPartitionSpec instance);

    @CanIgnoreReturnValue
    Builder specId(int specId);

    @CanIgnoreReturnValue
    Builder addField(IcebergPartitionField element);

    @CanIgnoreReturnValue
    Builder addFields(IcebergPartitionField... elements);

    @CanIgnoreReturnValue
    Builder fields(Iterable<? extends IcebergPartitionField> elements);

    @CanIgnoreReturnValue
    Builder addAllFields(Iterable<? extends IcebergPartitionField> elements);

    IcebergPartitionSpec build();
  }
}
