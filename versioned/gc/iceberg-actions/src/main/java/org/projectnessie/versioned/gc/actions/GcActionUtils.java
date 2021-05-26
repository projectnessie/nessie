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
package org.projectnessie.versioned.gc.actions;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class GcActionUtils {

  private GcActionUtils() {}

  /**
   * Fetch the most recent or largest runid for a gc table.
   *
   * <p>this assumes tableName is a table which has an integer runid.
   */
  public static long getMaxRunId(SparkSession spark, String tableName) {
    // equivalent to SELECT MAX(runid) from tableName
    Row maxRunId = spark.read().format("iceberg").load(tableName).groupBy().max("runid").first();
    return maxRunId.isNullAt(0) ? 0 : maxRunId.getLong(0);
  }
}
