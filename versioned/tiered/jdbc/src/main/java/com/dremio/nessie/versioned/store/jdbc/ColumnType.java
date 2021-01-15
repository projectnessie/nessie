/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.versioned.store.jdbc;

/**
 * Abstracts column types, so the actual {@link Dialect database-dialects} can implement the
 * type handling in a database-native way.
 */
enum ColumnType {
  ID,
  DT,
  SEQ,
  BINARY,
  BOOL,
  LONG,
  ID_LIST,
  KEY_LIST,
  KEY_MUTATION_LIST,
  KEY_DELTA_LIST,
  REF_TYPE,
  REF_NAME
}
