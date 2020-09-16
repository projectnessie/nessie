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

import java.util.Map;
import java.util.Optional;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.delta.sources.DeltaDataSource;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import com.dremio.nessie.iceberg.spark.NessieIcebergSource;
import com.google.common.base.Preconditions;

import scala.collection.JavaConverters;

public class NessieSource
      implements DataSourceV2, ReadSupport, WriteSupport, DataSourceRegister, CreatableRelationProvider, RelationProvider {

  enum TableType {
    HIVE,
    ICEBERG,
    DELTA,
    UNKNOWN
  }

  private final NessieIcebergSource icebergSource;
  private final DeltaDataSource deltaSource;

  public NessieSource() {
    icebergSource = new NessieIcebergSource();
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
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }

  private TableType getTableType(DataSourceOptions options) {
    return getTableType(options.asMap());
  }

  private TableType getTableType(Map<String, String> properties) {
    String path = properties.get("path");
    if (path == null) {
      return TableType.UNKNOWN;
    }
    String type = properties.get("nessie.file.type");
    try {
      return TableType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException | NullPointerException e) {
      //leave as unknown
    }
    if (path.contains("/")) {
      return TableType.DELTA;
    }
    try {
      TableIdentifier.parse(path);
      return TableType.ICEBERG;
    } catch (IllegalArgumentException e) {
      return TableType.UNKNOWN;
    }
  }

  @Override
  public BaseRelation createRelation(SQLContext sqlContext,
                                     SaveMode mode,
                                     scala.collection.immutable.Map<String, String> parameters,
                                     Dataset<Row> data) {
    //createRelation is only called via the delta path. So we make sure its delta and continue
    Preconditions.checkArgument(getTableType(JavaConverters.mapAsJavaMap(parameters)) == TableType.DELTA);
    return new DeltaDataSource().createRelation(sqlContext, mode, parameters, data);
  }

  @Override
  public BaseRelation createRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> parameters) {
    //createRelation is only called via the delta path. So we make sure its delta and continue
    Preconditions.checkArgument(getTableType(JavaConverters.mapAsJavaMap(parameters)) == TableType.DELTA);
    return new DeltaDataSource().createRelation(sqlContext, parameters);
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String writeUUID,
                                                 StructType schema,
                                                 SaveMode mode,
                                                 DataSourceOptions options) {
    TableType tableType = getTableType(options);
    switch (tableType) {
      case ICEBERG:
        return icebergSource.createWriter(writeUUID, schema, mode, options);
      case DELTA:
      case HIVE:
      default:
        throw new UnsupportedOperationException(String.format("Can't read type %s", tableType));
    }
  }
}
