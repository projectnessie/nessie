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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.projectnessie.model.Contents;
import org.projectnessie.model.HiveTable;
import org.projectnessie.model.ImmutableHiveTable;

import com.google.common.collect.ImmutableList;

class TableW extends Item {

  private final Table table;
  private final List<Partition> partitions;
  private final String uuid;

  public TableW(Table table, List<Partition> partitions, String uuid) {
    this.table = table;
    this.partitions = ImmutableList.copyOf(partitions);
    this.uuid = uuid;
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

  public static TableW fromContents(Contents c) {
    if (!(c instanceof HiveTable)) {
      throw new RuntimeException("Not a Hive table.");
    }

    HiveTable ht = (HiveTable) c;
    return Item.wrap(
        fromBytes(new Table(), ht.getTableDefinition()),
        ht.getPartitions().stream().map(p -> fromBytes(new Partition(), p)).collect(Collectors.toList()),
        c.getUuid()
        );
  }

  @Override
  public Contents toContents() {
    return ImmutableHiveTable.builder().uuid(uuid).tableDefinition(toBytes(table))
        .addAllPartitions(getPartitions()
            .stream()
            .map(Item::toBytes)
            .collect(Collectors.toList()))
            .build();
  }

  @Override
  public String getUuid() {
    return uuid;
  }
}
