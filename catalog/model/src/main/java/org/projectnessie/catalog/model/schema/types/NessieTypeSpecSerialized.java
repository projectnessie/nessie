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
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BINARY:
      case DATE:
      case UUID:
      case INTERVAL:
        break;
      case TIME:
        NessieTimeTypeSpec time = (NessieTimeTypeSpec) value;
        b.precision(time.precision()).withTimeZone(time.withTimeZone());
        break;
      case TIMESTAMP:
        NessieTimestampTypeSpec timestamp = (NessieTimestampTypeSpec) value;
        b.precision(timestamp.precision()).withTimeZone(timestamp.withTimeZone());
        break;
      case DECIMAL:
        NessieDecimalTypeSpec decimal = (NessieDecimalTypeSpec) value;
        b.length(decimal.scale()).precision(decimal.precision());
        break;
      case FIXED:
        NessieFixedTypeSpec fixed = (NessieFixedTypeSpec) value;
        b.length(fixed.length());
        break;
      case LIST:
        NessieListTypeSpec list = (NessieListTypeSpec) value;
        b.elementType(wrap(list.elementType()))
            .icebergElementFieldId(list.icebergElementFieldId())
            .elementsNullable(list.elementsNullable());
        break;
      case MAP:
        NessieMapTypeSpec map = (NessieMapTypeSpec) value;
        b.keyType(wrap(map.keyType())).elementType(wrap(map.valueType()));
        break;
      case STRUCT:
        NessieStructTypeSpec struct = (NessieStructTypeSpec) value;
        b.fields(struct.struct().fields()).icebergRecordName(struct.struct().icebergRecordName());
        break;
      default:
        throw new IllegalArgumentException("Unknown type " + nessieType);
    }
    return b.build();
  }

  @Value.NonAttribute
  default NessieTypeSpec unwrap() {
    NessieType nessieType = NessieType.valueOf(type().toUpperCase(Locale.ROOT));
    switch (nessieType) {
      case BOOLEAN:
        return NessieType.booleanType();
      case TINYINT:
        return NessieType.tinyintType();
      case SMALLINT:
        return NessieType.smallintType();
      case INT:
        return NessieType.intType();
      case BIGINT:
        return NessieType.bigintType();
      case FLOAT:
        return NessieType.floatType();
      case DOUBLE:
        return NessieType.doubleType();
      case STRING:
        return NessieType.stringType();
      case BINARY:
        return NessieType.binaryType();
      case DATE:
        return NessieType.dateType();
      case UUID:
        return NessieType.uuidType();
      case INTERVAL:
        return NessieType.intervalType();
      case TIME:
        return NessieType.timeType(Boolean.TRUE.equals(withTimeZone()));
      case TIMESTAMP:
        return NessieType.timestampType(Boolean.TRUE.equals(withTimeZone()));
      case DECIMAL:
        return NessieType.decimalType(requireNonNull(length()), requireNonNull(precision()));
      case FIXED:
        return NessieType.fixedType(requireNonNull(length()));
      case LIST:
        return NessieType.listType(
            requireNonNull(elementType()).unwrap(), icebergElementFieldId(), elementsNullable());
      case MAP:
        return NessieType.mapType(
            requireNonNull(keyType()).unwrap(),
            icebergKeyFieldId(),
            requireNonNull(elementType()).unwrap(),
            icebergElementFieldId(),
            elementsNullable());
      case STRUCT:
        return NessieType.structType(NessieStruct.nessieStruct(fields(), icebergRecordName()));
      default:
        throw new IllegalArgumentException("Unknown type " + nessieType);
    }
  }
}
