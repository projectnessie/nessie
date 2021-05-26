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
package org.projectnessie.hms;

import org.apache.hadoop.hive.metastore.api.Database;
import org.projectnessie.model.Contents;
import org.projectnessie.model.HiveDatabase;
import org.projectnessie.model.ImmutableHiveDatabase;

class DatabaseW extends Item {

  private final Database database;
  private final String id;

  public DatabaseW(Database database, String id) {
    super();
    this.database = database;
    this.id = id;
  }

  @Override
  public Type getType() {
    return Type.DATABASE;
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  @Override
  public HiveDatabase toContents() {
    return ImmutableHiveDatabase.builder().databaseDefinition(toBytes(database)).id(id).build();
  }

  public static DatabaseW fromContents(Contents c) {
    if (!(c instanceof HiveDatabase)) {
      throw new RuntimeException("Not a Hive datbaase.");
    }
    return new DatabaseW(
        fromBytes(new Database(), ((HiveDatabase) c).getDatabaseDefinition()), c.getId());
  }

  @Override
  public String getId() {
    return id;
  }
}
