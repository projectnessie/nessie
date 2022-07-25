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
package org.projectnessie.gc.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.sql.Timestamp;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.gc.base.BaseResultsRepo;

/** DDL + DML functionality for the "ExpiredResults" table. */
public final class ExpiredResultsRepo extends BaseResultsRepo {

  private static final String COL_GC_RUN_START = "gcRunStart";
  private static final String COL_GC_RUN_ID = "gcRunId";
  private static final String COL_CONTENT_ID = "contentId";
  private static final String COL_EXPIRED_FILES_TYPE = "expiredFilesType";
  private static final String COL_EXPIRED_FILES_COUNT = "expiredFilesCount";
  private static final String COL_EXPIRED_FILES_LIST = "expiredFilesList";

  private static final Schema icebergSchema =
      new Schema(
          Types.StructType.of(
                  // GC run start timestamp.
                  required(1, COL_GC_RUN_START, Types.TimestampType.withZone()),
                  // GC run-ID.
                  required(2, COL_GC_RUN_ID, Types.StringType.get()),
                  // Nessie Content.id
                  optional(3, COL_CONTENT_ID, Types.StringType.get()),
                  //  Expired file type: ICEBERG_MANIFEST, ICEBERG_MANIFESTLIST, DATA_FILE
                  optional(4, COL_EXPIRED_FILES_TYPE, Types.StringType.get()),
                  // Expired files count per type per content id.
                  optional(5, COL_EXPIRED_FILES_COUNT, Types.LongType.get()),
                  // List of expired files.
                  optional(
                      6,
                      COL_EXPIRED_FILES_LIST,
                      Types.ListType.ofOptional(7, Types.StringType.get())))
              .fields());

  public ExpiredResultsRepo(
      SparkSession sparkSession, String catalog, String gcBranchName, String gcTableIdentifier) {
    super(sparkSession, catalog, gcBranchName, gcTableIdentifier);
    createTableIfAbsent(
        sparkSession,
        catalog,
        TableIdentifier.parse(gcTableIdentifier),
        gcBranchName,
        icebergSchema);
  }

  /**
   * Collect the result of {@link ExpireContentsProcedure}.
   *
   * @param runId run id returned by {@link ExpireContentsProcedure}.
   * @return spark dataset of row where each row is expired files list per content id per type.
   */
  public Dataset<Row> collectExpiredResults(String runId, Timestamp timestamp) {
    // "startTime", "runID", "content_id", "deleted_files_type", "deleted_files_count",
    // "deleted_files_list"
    return sql(
        "SELECT * FROM %s WHERE %s = '%s' AND %s = '%s'",
        getCatalogAndTableWithRefName(),
        //
        COL_GC_RUN_ID,
        runId,
        //
        COL_GC_RUN_START,
        timestamp);
  }
}
