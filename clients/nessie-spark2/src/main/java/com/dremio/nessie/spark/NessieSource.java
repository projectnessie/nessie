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
package com.dremio.nessie.spark;

import java.util.List;

import org.apache.iceberg.spark.source.IcebergSource;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.delta.sources.DeltaDataSource;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.StructType;

public class NessieSource implements DataSourceV2, ReadSupport, DataSourceRegister {

  enum TableType {
    HIVE,
    ICEBERG,
    DELTA
  }

  private final IcebergSource icebergSource;
  private final DeltaDataSource deltaSource;

  public NessieSource() {
    icebergSource = new IcebergSource();
    deltaSource = new DeltaDataSource();
  }

  @Override
  public String shortName() {
    return "nessie";
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    TableType tableType = getTableType(options);
    switch (tableType) {
      case ICEBERG:
        return icebergSource.createReader(options);
      case DELTA:
        return new DeltaDataSourceReader(options, deltaSource);
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  private TableType getTableType(DataSourceOptions options) {
    return TableType.ICEBERG;
  }

  private class DeltaDataSourceReader implements DataSourceReader {

    private final DataSourceOptions options;
    private final DeltaDataSource deltaSource;

    public DeltaDataSourceReader(DataSourceOptions options, DeltaDataSource deltaSource) {
      this.options = options;
      this.deltaSource = deltaSource;
    }

    @Override
    public StructType readSchema() {
      return null;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      return null;
    }
  }
}
