/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.cassandra;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

public enum CqlColumnType {
  // 0
  NAME(DataTypes.TEXT),
  // 1
  OBJ_ID(DataTypes.ASCII),
  // 2
  OBJ_ID_LIST(DataTypes.listOf(DataTypes.TEXT)),
  // 3
  BOOL(DataTypes.BOOLEAN),
  // 4
  VARBINARY(DataTypes.BLOB),
  // 5
  BIGINT(DataTypes.BIGINT),
  // 6
  VARCHAR(DataTypes.TEXT),
  // 7
  INT(DataTypes.INT);

  private final DataType dataType;

  CqlColumnType(DataType dataType) {
    this.dataType = dataType;
  }

  public String cqlName() {
    return dataType.asCql(false, false);
  }

  public DataType dataType() {
    return dataType;
  }
}
