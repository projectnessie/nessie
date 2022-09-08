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
package org.projectnessie.gc.iceberg.inttest;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.gc.iceberg.inttest.Util.expire;
import static org.projectnessie.gc.iceberg.inttest.Util.identifyLiveContents;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.s3mock.IcebergS3Mock;
import org.projectnessie.s3mock.IcebergS3Mock.S3MockServer;
import org.projectnessie.s3mock.S3Bucket;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/**
 * Simulates Nessie GC running against a Nessie repository with many branches, contents, commits
 * plus Iceberg tables with many data files.
 *
 * <p>This "test" does not perform any correctness checks - the only reason that this "test" exists
 * is to validate that the integration of Iceberg code and Nessie GC works fine and is fast.
 *
 * <p>Utilizes "mocked" Iceberg objects served via a "mocked" S3, similar to {@link
 * org.projectnessie.gc.huge.TestManyObjects}.
 */
public class ITHuge {

  static Stream<Arguments> parameters() {
    return Stream.of(arguments(5, 100, 3, 250), arguments(20, 200, 3, 250));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void hugeDataLake(
      int refCount, int commitMultiplier, int retainedSnapshots, int filesPerSnapshot)
      throws Exception {

    HugeFiles files = new HugeFiles();

    try (S3MockServer s3Mock =
            IcebergS3Mock.builder()
                .putBuckets(
                    "foo", S3Bucket.builder().deleter(files).lister(files).object(files).build())
                .build()
                .start();
        IcebergFiles icebergFiles =
            IcebergFiles.builder().properties(s3Mock.icebergProperties()).build();
        HugeRepositoryConnector repositoryConnector =
            new HugeRepositoryConnector(
                refCount, commitMultiplier, retainedSnapshots, filesPerSnapshot)) {

      for (int i = 0; i < repositoryConnector.refCount; i++) {
        repositoryConnector
            .allReferences()
            .flatMap(repositoryConnector::commitLog)
            .forEach(
                logEntry -> {
                  List<Operation> ops = logEntry.getOperations();
                  if (ops != null) {
                    for (Operation op : ops) {
                      IcebergTable table = (IcebergTable) ((Operation.Put) op).getContent();
                      files.createSnapshot(table.getId(), (int) table.getSnapshotId());
                    }
                  }
                });
      }

      HugePersistenceSpi contentsStorage = new HugePersistenceSpi(repositoryConnector);

      Instant maxFileModificationTime = Instant.now();

      PerRefCutoffPolicySupplier cutOffPolicySupplier = r -> CutoffPolicy.NONE; // numCommits(3);

      // Mark...
      LiveContentSet liveContentSet =
          identifyLiveContents(contentsStorage, cutOffPolicySupplier, repositoryConnector);
      // ... and sweep
      DeleteSummary deleteSummary = expire(icebergFiles, liveContentSet, maxFileModificationTime);
      System.out.println(deleteSummary);
    }
  }
}
