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
package org.projectnessie.versioned.storage.cassandra;

import java.util.Objects;

public final class CqlColumn {

  private final String name;

  private final CqlColumnType type;

  public CqlColumn(String name, CqlColumnType type) {
    this.name = name;
    this.type = type;
  }

  public String name() {
    return name;
  }

  public CqlColumnType type() {
    return type;
  }

  @Override
  public String toString() {
    return name();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CqlColumn cqlColumn = (CqlColumn) o;
    return Objects.equals(name, cqlColumn.name) && type == cqlColumn.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }
}
