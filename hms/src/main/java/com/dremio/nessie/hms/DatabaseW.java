package com.dremio.nessie.hms;

import org.apache.hadoop.hive.metastore.api.Database;

class DatabaseW implements Item {

  private final Database database;

  public DatabaseW(Database database) {
    super();
    this.database = database;
  }

  @Override
  public Type getType() {
    return Type.DATABASE;
  }

  @Override
  public Database getDatabase() {
    return database;
  }


}
