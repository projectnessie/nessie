package com.dremio.nessie.hms;

import org.apache.hadoop.hive.metastore.api.Table;

import com.dremio.nessie.versioned.Key;

public class TableKey {

  private final String catalog;
  private final String database;
  private final String table;

  public TableKey(String catalog, String database, String table) {
    this.catalog = catalog;
    this.database = database;
    this.table = table;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  Key toKey() {
    return Key.of(database, table);
  }

  TableKey on(Table table) {

  }
}
