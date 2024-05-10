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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.projectnessie.catalog.model.schema.NessieStruct;

/** Base data types. */
public enum NessieType {
  BOOLEAN("boolean", NessieBooleanTypeSpec.class),
  // TODO refactor integer and float types to the "Parquet style" - so e.g. having attributes like
  //  "bits" and "signed" - and pre-define common 8/16/32/64 signed ones?
  // TODO add a type for java.math.BigDecimal ?
  TINYINT("tinyint", NessieTinyintTypeSpec.class),
  SMALLINT("smallint", NessieSmallintTypeSpec.class),
  INT("int", NessieIntTypeSpec.class),
  BIGINT("bigint", NessieBigintTypeSpec.class),
  FLOAT("float", NessieFloatTypeSpec.class),
  DOUBLE("double", NessieDoubleTypeSpec.class),
  UUID("uuid", NessieUuidTypeSpec.class),
  STRING("string", NessieStringTypeSpec.class),
  DATE("date", NessieDateTypeSpec.class),
  TIME("time", NessieTimeTypeSpec.class),
  TIMESTAMP("timestamp", NessieTimestampTypeSpec.class),
  INTERVAL("interval", NessieIntervalTypeSpec.class),
  DECIMAL("decimal", NessieDecimalTypeSpec.class),
  BINARY("binary", NessieBinaryTypeSpec.class),
  FIXED("fixed", NessieFixedTypeSpec.class),
  LIST("list", NessieListTypeSpec.class),
  MAP("map", NessieMapTypeSpec.class),
  STRUCT("struct", NessieStructTypeSpec.class);

  public static final int DEFAULT_TIME_PRECISION = 6;

  private final String lowerCaseName;
  private final Class<? extends NessieTypeSpec> typeSpec;

  NessieType(String lowerCaseName, Class<? extends NessieTypeSpec> typeSpec) {
    this.lowerCaseName = lowerCaseName;
    this.typeSpec = typeSpec;
  }

  public String lowerCaseName() {
    return lowerCaseName;
  }

  public Class<? extends NessieTypeSpec> typeSpec() {
    return typeSpec;
  }

  private static final NessieBinaryTypeSpec BINARY_TYPE =
      ImmutableNessieBinaryTypeSpec.builder().build();
  private static final NessieStringTypeSpec STRING_TYPE =
      ImmutableNessieStringTypeSpec.builder().build();
  private static final NessieBooleanTypeSpec BOOLEAN_TYPE =
      ImmutableNessieBooleanTypeSpec.builder().build();
  private static final NessieTinyintTypeSpec TINYINT_TYPE =
      ImmutableNessieTinyintTypeSpec.builder().build();
  private static final NessieSmallintTypeSpec SMALLINT_TYPE =
      ImmutableNessieSmallintTypeSpec.builder().build();
  private static final NessieIntTypeSpec INT_TYPE = ImmutableNessieIntTypeSpec.builder().build();
  private static final NessieBigintTypeSpec BIGINT_TYPE =
      ImmutableNessieBigintTypeSpec.builder().build();
  private static final NessieFloatTypeSpec FLOAT_TYPE =
      ImmutableNessieFloatTypeSpec.builder().build();
  private static final NessieDoubleTypeSpec DOUBLE_TYPE =
      ImmutableNessieDoubleTypeSpec.builder().build();
  private static final NessieUuidTypeSpec UUID_TYPE = ImmutableNessieUuidTypeSpec.builder().build();
  private static final NessieDateTypeSpec DATE_TYPE = ImmutableNessieDateTypeSpec.builder().build();
  private static final NessieTimeTypeSpec TIME_TYPE =
      ImmutableNessieTimeTypeSpec.builder()
          .withTimeZone(false)
          .precision(DEFAULT_TIME_PRECISION)
          .build();
  private static final NessieTimeTypeSpec TIME_WITH_TZ_TYPE =
      ImmutableNessieTimeTypeSpec.builder()
          .withTimeZone(true)
          .precision(DEFAULT_TIME_PRECISION)
          .build();
  private static final NessieTimestampTypeSpec TIMESTAMP_TYPE =
      ImmutableNessieTimestampTypeSpec.builder()
          .withTimeZone(false)
          .precision(DEFAULT_TIME_PRECISION)
          .build();
  private static final NessieTimestampTypeSpec TIMESTAMP_WITH_TZ_TYPE =
      ImmutableNessieTimestampTypeSpec.builder()
          .withTimeZone(true)
          .precision(DEFAULT_TIME_PRECISION)
          .build();
  private static final NessieIntervalTypeSpec INTERVAL_TYPE =
      ImmutableNessieIntervalTypeSpec.builder().build();

  private static final List<NessieTypeSpec> PRIMITIVE_TYPES =
      Collections.unmodifiableList(
          Arrays.asList(
              BINARY_TYPE,
              STRING_TYPE,
              BOOLEAN_TYPE,
              TINYINT_TYPE,
              SMALLINT_TYPE,
              INT_TYPE,
              BIGINT_TYPE,
              FLOAT_TYPE,
              DOUBLE_TYPE,
              UUID_TYPE,
              TIME_TYPE,
              TIME_WITH_TZ_TYPE,
              DATE_TYPE,
              TIMESTAMP_TYPE,
              TIMESTAMP_WITH_TZ_TYPE,
              INTERVAL_TYPE));

  public static List<? extends NessieTypeSpec> primitiveTypes() {
    return PRIMITIVE_TYPES;
  }

  public static NessieDecimalTypeSpec decimalType(int scale, int precision) {
    return ImmutableNessieDecimalTypeSpec.of(scale, precision);
  }

  public static NessieFixedTypeSpec fixedType(int length) {
    return ImmutableNessieFixedTypeSpec.of(length);
  }

  public static NessieBinaryTypeSpec binaryType() {
    return BINARY_TYPE;
  }

  public static NessieStringTypeSpec stringType() {
    return STRING_TYPE;
  }

  public static NessieBooleanTypeSpec booleanType() {
    return BOOLEAN_TYPE;
  }

  public static NessieTinyintTypeSpec tinyintType() {
    return TINYINT_TYPE;
  }

  public static NessieSmallintTypeSpec smallintType() {
    return SMALLINT_TYPE;
  }

  public static NessieIntTypeSpec intType() {
    return INT_TYPE;
  }

  public static NessieBigintTypeSpec bigintType() {
    return BIGINT_TYPE;
  }

  public static NessieFloatTypeSpec floatType() {
    return FLOAT_TYPE;
  }

  public static NessieDoubleTypeSpec doubleType() {
    return DOUBLE_TYPE;
  }

  public static NessieUuidTypeSpec uuidType() {
    return UUID_TYPE;
  }

  public static NessieDateTypeSpec dateType() {
    return DATE_TYPE;
  }

  public static NessieTimeTypeSpec timeType() {
    return TIME_TYPE;
  }

  public static NessieTimeTypeSpec timeType(boolean withTimeZone) {
    return withTimeZone ? TIME_WITH_TZ_TYPE : TIME_TYPE;
  }

  public static NessieTimeTypeSpec timeType(int precision, boolean withTimeZone) {
    return precision == DEFAULT_TIME_PRECISION
        ? timeType(withTimeZone)
        : ImmutableNessieTimeTypeSpec.of(precision, withTimeZone);
  }

  public static NessieTimestampTypeSpec timestampType(boolean withTimeZone) {
    return withTimeZone ? TIMESTAMP_WITH_TZ_TYPE : TIMESTAMP_TYPE;
  }

  public static NessieTimestampTypeSpec timestampType(int precision, boolean withTimeZone) {
    return precision == DEFAULT_TIME_PRECISION
        ? timestampType(withTimeZone)
        : ImmutableNessieTimestampTypeSpec.of(precision, withTimeZone);
  }

  public static NessieIntervalTypeSpec intervalType() {
    return INTERVAL_TYPE;
  }

  public static NessieListTypeSpec listType(
      NessieTypeSpec elementType, Integer icebergElementFieldId, Boolean elementsNullable) {
    return ImmutableNessieListTypeSpec.of(elementType, icebergElementFieldId, elementsNullable);
  }

  public static NessieMapTypeSpec mapType(
      NessieTypeSpec keyType,
      Integer icebergKeyFieldId,
      NessieTypeSpec valueType,
      Integer icebergValueFieldId,
      Boolean valuesNullable) {
    return ImmutableNessieMapTypeSpec.of(
        keyType, icebergKeyFieldId, valueType, icebergValueFieldId, valuesNullable);
  }

  public static NessieStructTypeSpec structType(NessieStruct struct) {
    return ImmutableNessieStructTypeSpec.of(struct);
  }
}
