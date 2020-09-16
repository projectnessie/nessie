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

import static com.dremio.nessie.spark.NessieSparkCatalog.getTableType;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.sources.DeltaDataSource;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import com.dremio.nessie.iceberg.spark.NessieIcebergSource;
import com.dremio.nessie.spark.NessieSparkCatalog.TableType;
import com.google.common.base.Preconditions;

import scala.collection.JavaConverters;

public class NessieSource implements DataSourceRegister, TableProvider, CreatableRelationProvider, RelationProvider {

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return this.getTable(null, null, options).partitioning();
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Table getTable(StructType schema,
                        Transform[] partitioning,
                        Map<String, String> properties) {
    TableType tableType = getTableType(properties);
    switch (tableType) {
      case ICEBERG:
        return new NessieIcebergSource().getTable(schema, partitioning, properties);
      case DELTA:
        return new DeltaDataSource().getTable(schema, partitioning, properties);
      case HIVE:
        //todo can I return a Hive source?
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException(String.format("Can't load a table of type %s", tableType));
    }
  }


  @Override
  public String shortName() {
    return "nessie";
  }

  public boolean supportsExternalMetadata() {
    return true;
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
}
