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
package org.projectnessie.spark.extensions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.TableReference;

public class ITNessieStatements extends AbstractNessieSparkSqlExtensionTest {

  @ParameterizedTest
  @CsvSource({
    "testCompaction,tbl",
    "main,tbl",
    "testCompaction,`tbl@testCompaction`",
    "main,`tbl@main`"
  })
  @Override
  void testCompaction(String branchName, String tableName) throws NessieNotFoundException {
    String branchHash = prepareForCompaction(branchName);

    // In Iceberg versions >= 0.14.0 with Spark versions <= 3.1,
    if (TableReference.parse(tableName).hasReference()) {
      // Iceberg compaction procedure parse the table identifier using spark parser
      // and fails because of backtick syntax used in table reference.
      // Hence, compaction throws parsing error.
      // In the newer versions, because of SparkCachedTableCatalog,
      // table identifier is mapped to UUID and Spark parses UUID successfully.
      assertThatThrownBy(() -> executeCompaction(tableName))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              String.format("Cannot parse path or identifier: nessie.db.tbl@%s", branchName));
    } else {
      // On use reference cmd path, for non-default reference, compaction throws error.
      if (!branchName.equals("main")) {
        // DataframeReader in Iceberg compaction code uses SparkCatalog, which builds the
        // IcebergCatalog using the original catalog conf from SQLConf instead of active session
        // conf.
        // As USE REFERENCE command only updates branch name in active session conf,
        // compaction will try to use original reference ("main") instead of the one from USE
        // REFERENCE.
        // Hence, compaction throws table not found error.
        assertThatThrownBy(() -> executeCompaction(tableName))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Table db.tbl not found");
      } else {
        // compaction on default branch.
        executeAndValidateCompaction(branchName, branchHash, tableName);
      }
    }
  }
}
