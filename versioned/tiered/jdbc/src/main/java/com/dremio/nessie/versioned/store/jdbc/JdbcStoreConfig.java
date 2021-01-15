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

import javax.annotation.Nullable;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class JdbcStoreConfig {

  // DEFAULT TABLE PREFIXNAMES
  public static final String TABLE_PREFIX = "nessie_";

  @Default
  public String getTablePrefix() {
    return TABLE_PREFIX;
  }

  /**
   * Create the Nessie database tables, if those do not already exist. Defaults to {@code true}.
   * @return whether to setup the database tables.
   */
  @Default
  public boolean setupTables() {
    return true;
  }

  /**
   * Dump the DDL needed to create the Nessie database tables. Defaults to {@code true}.
   * Note that all DDL statements will be logged at {@code INFO} level.
   * @return whether to dump the DDL statement.
   */
  @Default
  public boolean logCreateDDL() {
    return true;
  }

  /**
   * Initializes the database with the initial values. Defaults to {@code true}.
   * @return whether to create the initial L1, L2 and L3 objects, if those do not already exists.
   */
  @Default
  public boolean initializeDatabase() {
    return true;
  }

  /**
   * The database catalog to use. Defaults to {@code null}.
   * @return Database catalog
   */
  @Default
  @Nullable
  public String getCatalog() {
    return null;
  }

  /**
   * The database schema to use. Defaults to {@code null}.
   * @return Database schema
   */
  @Default
  @Nullable
  public String getSchema() {
    return null;
  }

  /**
   * The dialect to use. Defaults to {@link Dialect#H2}
   * @return database dialect
   */
  @Default
  public Dialect getDialect() {
    return Dialect.H2;
  }

  public static ImmutableJdbcStoreConfig.Builder builder() {
    return ImmutableJdbcStoreConfig.builder();
  }
}
