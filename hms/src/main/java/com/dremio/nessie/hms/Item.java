package com.dremio.nessie.hms;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public interface Item {
  enum Type {CATALOG, DATABASE, TABLE}

  Type getType();

  default Catalog getCatalog() {
    throw new IllegalArgumentException("Not a catalog.");
  }

  default Table getTable() {
    throw new IllegalArgumentException("Not a table.");
  }

  default List<Partition> getPartitions() {
    throw new IllegalArgumentException("Not a table.");
  }

  default Database getDatabase() {
    throw new IllegalArgumentException("Not a database.");
  }

  static DatabaseW wrap(Database database) {
    return new DatabaseW(database);
  }

  static CatalogW wrap(Catalog catalog) {
    return new CatalogW(catalog);
  }


  static TableW wrap(Table table, List<Partition> partitions) {
    return new TableW(table, partitions);
  }
}
