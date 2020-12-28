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

import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.HiveDatabase;
import com.dremio.nessie.model.HiveTable;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

abstract class Item {
  public static enum Type {
    CATALOG,
    DATABASE,
    TABLE
  }

  public abstract Type getType();

  public abstract Contents toContents();

  public Table getTable() {
    throw new IllegalArgumentException("Not a table.");
  }

  public List<Partition> getPartitions() {
    throw new IllegalArgumentException("Not a table.");
  }

  public Database getDatabase() {
    throw new IllegalArgumentException("Not a database.");
  }

  static DatabaseW wrap(Database database) {
    return new DatabaseW(database);
  }

  static TableW wrap(Table table, List<Partition> partitions) {
    return new TableW(table, partitions);
  }

  static Item fromContents(Contents c) {
    if (c instanceof HiveTable) {
      return TableW.fromContents(c);
    } else if (c instanceof HiveDatabase) {
      return DatabaseW.fromContents(c);
    } else {
      // TODO: support translation of Iceberg and Delta native tables.
      throw new RuntimeException("Unable to convert to known value.");
    }
  }

  static byte[] toBytes(TBase<?, ?> base) {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      return serializer.serialize(base);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  static <T extends TBase<T, ?>> T fromBytes(T empty, byte[] bytes) {
    TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    try {
      deserializer.deserialize(empty, bytes);
      return empty;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
