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
package org.projectnessie.catalog.model.schema.types;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieStruct;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieTypeSpecSerialized.class)
@JsonDeserialize(as = ImmutableNessieTypeSpecSerialized.class)
public interface NessieTypeSpecSerialized {
  String type();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - big-decimal (scale)
  // - fixed (length)
  Integer length();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - big-decimal
  // - time
  // - timestamp
  Integer precision();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - time
  // - timestamp
  Boolean withTimeZone();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - list
  // - map (value)
  NessieTypeSpecSerialized elementType();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - list
  // - map (value)
  Integer icebergElementFieldId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - list
  // - map (value)
  Boolean elementsNullable();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - map
  NessieTypeSpecSerialized keyType();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Used for:
  // - map
  Integer icebergKeyFieldId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  // Used for:
  // - struct (fields)
  List<NessieField> fields();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String icebergRecordName();

  static NessieTypeSpecSerialized wrap(NessieTypeSpec value) {
    NessieType nessieType = value.type();
    ImmutableNessieTypeSpecSerialized.Builder b =
        ImmutableNessieTypeSpecSerialized.builder().type(nessieType.lowerCaseName());
    switch (nessieType) {
      case BOOLEAN,
          TINYINT,
          SMALLINT,
          INT,
          BIGINT,
          FLOAT,
          DOUBLE,
          STRING,
          BINARY,
          DATE,
          UUID,
          INTERVAL -> {}
      case TIME -> {
        NessieTimeTypeSpec time = (NessieTimeTypeSpec) value;
        b.precision(time.precision()).withTimeZone(time.withTimeZone());
      }
      case TIMESTAMP -> {
        NessieTimestampTypeSpec timestamp = (NessieTimestampTypeSpec) value;
        b.precision(timestamp.precision()).withTimeZone(timestamp.withTimeZone());
      }
      case DECIMAL -> {
        NessieDecimalTypeSpec decimal = (NessieDecimalTypeSpec) value;
        b.length(decimal.scale()).precision(decimal.precision());
      }
      case FIXED -> {
        NessieFixedTypeSpec fixed = (NessieFixedTypeSpec) value;
        b.length(fixed.length());
      }
      case LIST -> {
        NessieListTypeSpec list = (NessieListTypeSpec) value;
        b.elementType(wrap(list.elementType()))
            .icebergElementFieldId(list.icebergElementFieldId())
            .elementsNullable(list.elementsNullable());
      }
      case MAP -> {
        NessieMapTypeSpec map = (NessieMapTypeSpec) value;
        b.keyType(wrap(map.keyType())).elementType(wrap(map.valueType()));
      }
      case STRUCT -> {
        NessieStructTypeSpec struct = (NessieStructTypeSpec) value;
        b.fields(struct.struct().fields()).icebergRecordName(struct.struct().icebergRecordName());
      }
      default -> throw new IllegalArgumentException("Unknown type " + nessieType);
    }
    return b.build();
  }

  @Value.NonAttribute
  default NessieTypeSpec unwrap() {
    NessieType nessieType = NessieType.valueOf(type().toUpperCase(Locale.ROOT));
    return switch (nessieType) {
      case BOOLEAN -> NessieType.booleanType();
      case TINYINT -> NessieType.tinyintType();
      case SMALLINT -> NessieType.smallintType();
      case INT -> NessieType.intType();
      case BIGINT -> NessieType.bigintType();
      case FLOAT -> NessieType.floatType();
      case DOUBLE -> NessieType.doubleType();
      case STRING -> NessieType.stringType();
      case BINARY -> NessieType.binaryType();
      case DATE -> NessieType.dateType();
      case UUID -> NessieType.uuidType();
      case INTERVAL -> NessieType.intervalType();
      case TIME -> NessieType.timeType(Boolean.TRUE.equals(withTimeZone()));
      case TIMESTAMP -> NessieType.timestampType(Boolean.TRUE.equals(withTimeZone()));
      case DECIMAL -> NessieType.decimalType(requireNonNull(length()), requireNonNull(precision()));
      case FIXED -> NessieType.fixedType(requireNonNull(length()));
      case LIST ->
          NessieType.listType(
              requireNonNull(elementType()).unwrap(), icebergElementFieldId(), elementsNullable());
      case MAP ->
          NessieType.mapType(
              requireNonNull(keyType()).unwrap(),
              icebergKeyFieldId(),
              requireNonNull(elementType()).unwrap(),
              icebergElementFieldId(),
              elementsNullable());
      case STRUCT ->
          NessieType.structType(NessieStruct.nessieStruct(fields(), icebergRecordName()));
      default -> throw new IllegalArgumentException("Unknown type " + nessieType);
    };
  }
}
