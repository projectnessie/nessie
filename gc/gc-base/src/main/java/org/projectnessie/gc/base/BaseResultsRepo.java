/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.base;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.projectnessie.model.ImmutableTableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseResultsRepo {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseResultsRepo.class);

  private final SparkSession sparkSession;
  private final String catalogAndTableWithRefName;

  public BaseResultsRepo(
      SparkSession sparkSession, String catalog, String gcBranchName, String gcTableIdentifier) {
    this.sparkSession = sparkSession;
    this.catalogAndTableWithRefName = withRefName(catalog, gcTableIdentifier, gcBranchName);
  }

  public String getCatalogAndTableWithRefName() {
    return catalogAndTableWithRefName;
  }

  @FormatMethod
  public Dataset<Row> sql(@FormatString String sqlStatement, Object... args) {
    String sql = String.format(sqlStatement, args);
    LOGGER.debug("Executing the sql -> {}", sql);
    return sparkSession.sql(sql);
  }

  public void writeToOutputTable(Dataset<Row> rowDataset) {
    try {
      // write content rows to the output table
      rowDataset.writeTo(catalogAndTableWithRefName).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          "Problem while writing output rows to the table: " + catalogAndTableWithRefName, e);
    }
  }

  public static void createTableIfAbsent(
      SparkSession sparkSession,
      String catalogName,
      TableIdentifier tableIdentifier,
      String gcBranchName,
      Schema icebergSchema) {
    try {
      GCUtil.loadNessieCatalog(sparkSession, catalogName, gcBranchName)
          .createTable(tableIdentifier, icebergSchema);
    } catch (AlreadyExistsException ex) {
      // Table can exist from previous GC run, no need to throw exception.
    }
  }

  private static String withRefName(String catalog, String identifier, String refName) {
    int tableNameIndex = identifier.lastIndexOf(".");
    String namespace = identifier.substring(0, tableNameIndex);
    String tableName = identifier.substring(tableNameIndex + 1);
    return catalog
        + "."
        + namespace
        + "."
        + ImmutableTableReference.builder().name(tableName).reference(refName).build();
  }
}
