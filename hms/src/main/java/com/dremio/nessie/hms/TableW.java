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
