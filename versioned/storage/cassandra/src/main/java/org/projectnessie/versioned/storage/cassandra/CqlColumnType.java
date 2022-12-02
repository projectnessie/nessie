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

public enum CqlColumnType {
  // 0
  NAME("TEXT"),
  // 1
  OBJ_ID("ASCII"),
  // 2
  OBJ_ID_LIST("LIST<TEXT>"),
  // 3
  BOOL("BOOLEAN"),
  // 4
  VARBINARY("BLOB"),
  // 5
  BIGINT("BIGINT"),
  // 6
  VARCHAR("TEXT"),
  // 7
  INT("INT");

  private final String type;

  CqlColumnType(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }
}
