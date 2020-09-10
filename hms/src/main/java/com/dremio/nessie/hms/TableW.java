package com.dremio.nessie.hms;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import com.google.common.collect.ImmutableList;

class TableW implements Item {

  private final Table table;
  private final List<Partition> partitions;

  public TableW(Table table, List<Partition> partitions) {
    this.table = table;
    this.partitions = ImmutableList.copyOf(partitions);
  }

  @Override
  public Type getType() {
    return Type.TABLE;
  }

  @Override
  public Table getTable() {
    return table;
  }

  @Override
  public List<Partition> getPartitions() {
    return partitions;
  }

}
