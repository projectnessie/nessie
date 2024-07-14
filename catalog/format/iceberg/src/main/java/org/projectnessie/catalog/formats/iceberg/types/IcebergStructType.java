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
package org.projectnessie.catalog.formats.iceberg.types;

import static org.projectnessie.catalog.formats.iceberg.manifest.Avro.avroNullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergStructType.class)
@JsonDeserialize(as = ImmutableIcebergStructType.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(IcebergStructType.TYPE_NAME)
public interface IcebergStructType extends IcebergComplexType {
  String ICEBERG_FIELD_NAME_PROP = "iceberg-field-name";
  String FIELD_ID_PROP = "field-id";
  String TYPE_NAME = "struct";

  @Override
  @Value.Default
  default String type() {
    return TYPE_NAME;
  }

  List<IcebergNestedField> fields();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonIgnore
  String avroRecordName();

  @Override
  @Value.Lazy
  default Schema avroSchema(int fieldId) {
    // TODO Iceberg computes recordName a little different, see
    //  org.apache.iceberg.avro.TypeToSchema.struct
    String recordName = avroRecordName();
    if (recordName == null) {
      recordName = "r" + fieldId;
    }

    List<Schema.Field> fields = new ArrayList<>();
    for (IcebergNestedField structField : fields()) {
      String avroName = structField.avroName();

      Schema avroSchema = structField.avroSchema();

      Schema.Field field =
          structField.required()
              ? new Schema.Field(avroName, avroSchema, structField.doc())
              : new Schema.Field(
                  avroName, avroNullable(avroSchema), structField.doc(), JsonProperties.NULL_VALUE);

      if (!avroName.equals(structField.name())) {
        field.addProp(ICEBERG_FIELD_NAME_PROP, structField.name());
      }
      field.addProp(FIELD_ID_PROP, structField.id());
      fields.add(field);
    }

    return Schema.createRecord(recordName, null, null, false, fields);
  }

  static Builder builder() {
    return ImmutableIcebergStructType.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergType instance);

    @CanIgnoreReturnValue
    Builder type(String type);

    @CanIgnoreReturnValue
    Builder addField(IcebergNestedField element);

    @CanIgnoreReturnValue
    Builder addFields(IcebergNestedField... elements);

    @CanIgnoreReturnValue
    Builder fields(Iterable<? extends IcebergNestedField> elements);

    @CanIgnoreReturnValue
    Builder addAllFields(Iterable<? extends IcebergNestedField> elements);

    IcebergStructType build();
  }
}
