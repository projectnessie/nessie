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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = IcebergViewRepresentation.IcebergSQLViewRepresentation.class,
      name = "sql"),
})
public interface IcebergViewRepresentation {

  @JsonIgnore
  @Value.NonAttribute
  String representationKey();

  @NessieImmutable
  @JsonTypeName("sql")
  @JsonSerialize(as = ImmutableIcebergSQLViewRepresentation.class)
  @JsonDeserialize(as = ImmutableIcebergSQLViewRepresentation.class)
  interface IcebergSQLViewRepresentation extends IcebergViewRepresentation {
    String sql();

    String dialect();

    @Override
    default String representationKey() {
      return dialect();
    }

    static IcebergSQLViewRepresentation icebergSqlViewRepresentation(String sql, String dialect) {
      return ImmutableIcebergSQLViewRepresentation.of(sql, dialect);
    }
  }
}
