package com.dremio.nessie.hms;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TBase;

class CatalogW implements Item {

  private final Catalog catalog;

  public CatalogW(Catalog catalog) {
    super();
    this.catalog = catalog;
  }

  @Override
  public Type getType() {
    return Type.CATALOG;
  }

  @Override
  public Catalog getCatalog() {
    return catalog;
  }



}
