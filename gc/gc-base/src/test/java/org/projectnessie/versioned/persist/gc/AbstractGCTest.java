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
package org.projectnessie.versioned.persist.gc;

import static java.util.Arrays.asList;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;

@ExtendWith(DatabaseAdapterExtension.class)
abstract class AbstractGCTest {
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;

  @ParameterizedTest
  @MethodSource("datasetsSource")
  void testDataset(Dataset dataset) {
    // For every dataset in produceDatasetsSource, perform GC and validate the results.
    dataset.applyToAdapter(databaseAdapter);
    performGc(dataset, databaseAdapter);
  }

  static List<Dataset> datasetsSource() {
    return produceDatasetsSource(Dataset::new);
  }

  static final String DEFAULT_BRANCH = "main";
  static final Instant CUTOFF_TIMESTAMP = Instant.ofEpochSecond(500);
  // content keys
  static ContentKey TABLE_ONE = ContentKey.of("table", "one");
  static ContentKey TABLE_TWO = ContentKey.of("table", "two");
  static ContentKey TABLE_THREE = ContentKey.of("table", "three");
  static ContentKey TABLE_FOUR = ContentKey.of("table", "four");
  // content ids for the table keys
  static String CID_ONE = "table-one";
  static String CID_TWO = "table-two";
  static String CID_THREE = "table-three";

  /**
   * Common test implementation to verify the expected GC results for a specific data set. This test
   * is exactly the same for mocked and "real" {@link DatabaseAdapter}s, datasets are exactly the
   * same, timestamps are exactly the same, hashes however do vary.
   */
  protected void performGc(Dataset dataset, DatabaseAdapter databaseAdapter) {
    // perform GC on the dataset
    ContentValuesCollector<BasicExpiredContentValues> contentValuesCollector =
        new ContentValuesCollector<>(BasicExpiredContentValues::new);
    GC gc =
        GC.builder()
            .withApi(new NessieApiMock(databaseAdapter).getApi())
            .withDefaultCutOffTimeStamp(CUTOFF_TIMESTAMP)
            .withCutOffTimeStampPerReferenceFunc(dataset.cutOffTimeStampPerRefFunc)
            .build();
    GCResult<BasicExpiredContentValues> gcResult = gc.performGC(contentValuesCollector);
    // compare the expected contents against the actual gc output
    dataset.verify(gcResult);
  }

  /** Produces the datasets verified via {@link #performGc(Dataset, DatabaseAdapter)}. */
  static List<Dataset> produceDatasetsSource(Function<String, Dataset> datasetFactory) {
    // add each test scenario as one dataset below.
    return asList(
        testSingleTableSingleRef(datasetFactory.apply("testSingleTableSingleRef")),
        testSingleTableMultipleRef(datasetFactory.apply("testSingleTableMultipleRef")),
        testDropTableSingleRef(datasetFactory.apply("testDropTableSingleRef")),
        testMultipleTableMultipleRef(datasetFactory.apply("testMultipleTableMultipleRef")),
        testPerRefCutoffTime(datasetFactory.apply("testPerRefCutoffTime")),
        testTableRename(datasetFactory.apply("testTableRename")),
        testMixedContentTypes(datasetFactory.apply("testMixedContentTypes")));
  }

  // Note: If the content is expected to be expired after the gc (by applying GC algorithm), we are
  // setting
  // expectExpired flag as true in putContent() operations. So that, this input content is added for
  // validation by
  // comparing with gc output content.

  static Dataset testSingleTableSingleRef(Dataset ds) {
    return ds
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(10))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(8))
        .putContent(TABLE_ONE, IcebergTable.of("meta2", 43, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP)
        .putContent(TABLE_ONE, IcebergTable.of("meta3", 44, 42, 42, 42, CID_ONE), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(2))
        .putContent(TABLE_ONE, IcebergTable.of("meta4", 45, 42, 42, 42, CID_ONE), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(4))
        .putContent(TABLE_ONE, IcebergTable.of("meta5", 46, 42, 42, 42, CID_ONE), false)
        .commit();
  }

  static Dataset testSingleTableMultipleRef(Dataset ds) {
    return ds
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(10))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .switchToBranch("branch-1")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(8))
        .putContent(TABLE_ONE, IcebergTable.of("meta2", 43, 42, 42, 42, CID_ONE), false)
        .commit()
        //
        .switchToBranch(DEFAULT_BRANCH)
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(6))
        .putContent(TABLE_ONE, IcebergTable.of("meta3", 44, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(2))
        .putContent(TABLE_ONE, IcebergTable.of("meta4", 45, 42, 42, 42, CID_ONE), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(4))
        .putContent(TABLE_ONE, IcebergTable.of("meta5", 46, 42, 42, 42, CID_ONE), false)
        .commit();
  }

  static Dataset testDropTableSingleRef(Dataset ds) {
    return ds
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(10))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(12))
        .putContent(TABLE_ONE, IcebergTable.of("meta2", 43, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(14))
        .putContent(TABLE_ONE, IcebergTable.of("meta3", 46, 42, 42, 42, CID_ONE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(16))
        .deleteContent(TABLE_ONE)
        .commit();
  }

  static Dataset testMultipleTableMultipleRef(Dataset ds) {
    return ds
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(2))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .putContent(TABLE_TWO, IcebergTable.of("meta2", 42, 42, 42, 42, CID_TWO), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(1))
        .deleteContent(TABLE_TWO)
        .commit()
        // commit unchanged operation
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(1))
        .putContent(TABLE_THREE, IcebergTable.of("meta3", 42, 42, 42, 42, CID_THREE), false)
        .commit()
        //
        .switchToBranch("branch-1")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(50))
        .deleteContent(TABLE_ONE)
        .commit()
        //
        .switchToBranch("branch-2")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(100))
        .deleteContent(TABLE_THREE)
        .commit();
  }

  static Dataset testPerRefCutoffTime(Dataset ds) {
    return ds
        //
        .withCutOffTimeStampPerRefFunc(
            (ref) -> {
              ImmutableMap<String, Instant> map =
                  ImmutableMap.<String, Instant>builder()
                      .put("branch-1", CUTOFF_TIMESTAMP.plusSeconds(5))
                      .put("branch-2", CUTOFF_TIMESTAMP.plusSeconds(10))
                      .build();
              return map.get(ref.getName());
            })
        // Note: Another way to add withCutOffTimeStampPerRefFunc function is as below
        // .withCutOffTimeStampPerRefFunc(
        //   ref -> {
        //     if (ref.getName().equals("branch-1")) {
        //       return NOT_LIVE.plusSeconds(5);
        //     } else if ((ref.getName().equals("branch-2"))) {
        //       return NOT_LIVE.plusSeconds(10);
        //     } else {
        //       return null;
        //     }
        //   })
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(2))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .putContent(TABLE_TWO, IcebergTable.of("meta2", 42, 42, 42, 42, CID_TWO), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(1))
        .deleteContent(TABLE_TWO)
        .commit()
        // commit unchanged operation
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(1))
        .putContent(TABLE_THREE, IcebergTable.of("meta3", 42, 42, 42, 42, CID_THREE), false)
        .commit()
        //
        .switchToBranch("branch-1")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(3))
        .putContent(TABLE_THREE, IcebergTable.of("meta4", 43, 42, 42, 42, CID_THREE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(4))
        .putContent(TABLE_THREE, IcebergTable.of("meta5", 44, 42, 42, 42, CID_THREE), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(6))
        .putContent(TABLE_THREE, IcebergTable.of("meta5", 45, 42, 42, 42, CID_THREE), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(7))
        .putContent(TABLE_THREE, IcebergTable.of("meta5", 46, 42, 42, 42, CID_THREE), false)
        .commit()
        //
        .switchToBranch("branch-2")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(3))
        .putContent(TABLE_TWO, IcebergTable.of("meta4", 43, 42, 42, 42, CID_TWO), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(4))
        .putContent(TABLE_TWO, IcebergTable.of("meta5", 44, 42, 42, 42, CID_TWO), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(16))
        .putContent(TABLE_TWO, IcebergTable.of("meta5", 45, 42, 42, 42, CID_TWO), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(17))
        .putContent(TABLE_TWO, IcebergTable.of("meta5", 46, 42, 42, 42, CID_TWO), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(50))
        .deleteContent(TABLE_THREE)
        .commit();
  }

  static Dataset testTableRename(Dataset ds) {
    return ds
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(2))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .putContent(TABLE_TWO, IcebergTable.of("meta2", 42, 42, 42, 42, CID_TWO), true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(1))
        .deleteContent(TABLE_TWO)
        .commit()
        // commit unchanged operation
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(1))
        .putContent(TABLE_THREE, IcebergTable.of("meta3", 42, 42, 42, 42, CID_THREE), false)
        .commit()
        //
        .switchToBranch("branch-1")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(51))
        .deleteContent(TABLE_ONE)
        .commit()
        //
        .switchToBranch("branch-2")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(52))
        .deleteContent(TABLE_ONE)
        .putContent(TABLE_FOUR, IcebergTable.of("meta2_1", 43, 42, 42, 42, CID_TWO), false)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(53))
        .deleteContent(TABLE_THREE)
        .commit();
  }

  static Dataset testMixedContentTypes(Dataset ds) {
    return ds
        // commit both iceberg and deltalake content
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(2))
        .putContent(TABLE_ONE, IcebergTable.of("meta1", 42, 42, 42, 42, CID_ONE), false)
        .putContent(
            TABLE_TWO,
            ImmutableDeltaLakeTable.builder().id(CID_TWO).lastCheckpoint("ch2").build(),
            true)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.minusSeconds(1))
        .deleteContent(TABLE_TWO)
        .commit()
        // commit unchanged operation
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP)
        .commit()
        //
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(1))
        .putContent(
            TABLE_THREE,
            ImmutableDeltaLakeTable.builder().id(CID_THREE).lastCheckpoint("ch3").build(),
            false)
        .commit()
        //
        .switchToBranch("branch-1")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(50))
        .deleteContent(TABLE_ONE)
        .commit()
        //
        .switchToBranch("branch-2")
        .getCommitBuilderWithCutoffTime(CUTOFF_TIMESTAMP.plusSeconds(100))
        .deleteContent(TABLE_THREE)
        .commit();
  }
}
