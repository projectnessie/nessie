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
package org.projectnessie.versioned.storage.jdbc2;

import java.sql.SQLException;
import java.util.Map;

public interface DatabaseSpecific {

  Map<Jdbc2ColumnType, String> columnTypes();

  Map<Jdbc2ColumnType, Integer> columnTypeIds();

  boolean isConstraintViolation(SQLException e);

  boolean isRetryTransaction(SQLException e);

  boolean isAlreadyExists(SQLException e);

  String wrapInsert(String sql);

  String primaryKeyCol(String col, Jdbc2ColumnType columnType);
}
