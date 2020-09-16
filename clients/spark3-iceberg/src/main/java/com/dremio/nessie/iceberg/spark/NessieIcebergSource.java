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
package com.dremio.nessie.iceberg.spark;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.source.IcebergSource;

import com.dremio.nessie.iceberg.NessieCatalog;
import com.google.common.base.Preconditions;

/**
 * Get a table from an Iceberg Nessie Catalog.
 */
public class NessieIcebergSource extends IcebergSource {

  @Override
  protected Table findTable(Map<String, String> options, Configuration conf) {
    NessieCatalog catalog = new NessieCatalog(conf);
    String path = options.get("path");
    Preconditions.checkArgument(path != null, "Cannot open table: path is not set");
    TableIdentifier tableIdentifier = TableIdentifier.parse(path);
    return catalog.loadTable(tableIdentifier);
  }

  @Override
  public String shortName() {
    return "nessie-iceberg";
  }
}
