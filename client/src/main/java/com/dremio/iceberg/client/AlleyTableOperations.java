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
package com.dremio.iceberg.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

import com.dremio.iceberg.model.Table;

public class AlleyTableOperations extends BaseMetastoreTableOperations {

  private final Configuration conf;
  private final AlleyClient client;
  private Table table;
  private HadoopFileIO fileIO;

  public AlleyTableOperations(Configuration conf, Table table) {
    this.conf = conf;
    this.table = table;
    client = new AlleyClient(conf);
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = getMetadataForTable(conf, table);

    refreshFromMetadataLocation(metadataLocation);
  }

  private String getMetadataForTable(Configuration config, Table inTable) {
    return table.getMetadataLocation();
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    try {
      if (table.getEtag() == null) {
        Table modifiedTable = table.newMetadataLocation(newMetadataLocation);
        table = client.createTable(modifiedTable);
      } else {
        Table modifiedTable = table.newMetadataLocation(newMetadataLocation);
        client.updateTable(modifiedTable);
        table = client.getTable(table.getUuid());
      }
    } catch (Throwable e) {
      io().deleteFile(newMetadataLocation);
      throw new CommitFailedException(e, "failed");
    }
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }
}
