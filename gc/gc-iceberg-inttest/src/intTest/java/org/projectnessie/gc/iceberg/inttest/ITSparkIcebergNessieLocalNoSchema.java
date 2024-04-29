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

import static org.projectnessie.gc.iceberg.inttest.Util.expire;
import static org.projectnessie.gc.iceberg.inttest.Util.identifyLiveContents;

import java.io.File;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.NessieFileIOException;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.repository.NessieRepositoryConnector;
import org.projectnessie.spark.extensions.NessieSparkSessionExtensions;
import org.projectnessie.spark.extensions.SparkSqlTestBase;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
@DisabledOnOs(
    value = OS.WINDOWS,
    disabledReason = "a schemaless warehouse URI cannot be used on Windows")
public class ITSparkIcebergNessieLocalNoSchema extends SparkSqlTestBase {

  @InjectSoftAssertions private SoftAssertions soft;

  @TempDir File tempFile;

  @BeforeAll
  protected static void useNessieExtensions() {
    conf.set("spark.sql.extensions", NessieSparkSessionExtensions.class.getCanonicalName());
  }

  @Override
  protected String warehouseURI() {
    return tempFile.getPath();
  }

  @Test
  public void roundTripLocal() throws Exception {
    try (IcebergFiles icebergFiles = IcebergFiles.builder().build()) {

      api.createNamespace().namespace("db1").refName(api.getConfig().getDefaultBranch()).create();

      sql("create table nessie.db1.t1(id int) using iceberg");
      sql("insert into nessie.db1.t1 select 42");
      sql("insert into nessie.db1.t1 select 42");
      sql("select * from nessie.db1.t1");
      sql("describe formatted nessie.db1.t1");

      Set<StorageUri> filesBefore = allFiles(icebergFiles);

      Instant maxFileModificationTime = Instant.now();

      // All commits are considered live (CutoffPolicy.NONE)

      // Mark...
      LiveContentSet liveContentSet =
          identifyLiveContents(
              new InMemoryPersistenceSpi(),
              ref -> CutoffPolicy.NONE,
              NessieRepositoryConnector.nessie(api));
      // ... and sweep
      DeleteSummary deleteSummary = expire(icebergFiles, liveContentSet, maxFileModificationTime);
      soft.assertThat(deleteSummary.deleted()).isEqualTo(0L);
      Set<StorageUri> filesAfter = allFiles(icebergFiles);
      soft.assertThat(filesAfter).containsExactlyElementsOf(filesBefore);

      // Only the last commit is considered live:

      // Mark...
      liveContentSet =
          identifyLiveContents(
              new InMemoryPersistenceSpi(),
              ref -> CutoffPolicy.numCommits(1),
              NessieRepositoryConnector.nessie(api));
      // ... and sweep
      deleteSummary = expire(icebergFiles, liveContentSet, maxFileModificationTime);
      soft.assertThat(deleteSummary.deleted()).isEqualTo(3L);
      filesAfter = allFiles(icebergFiles);
      Set<StorageUri> removedFiles = new HashSet<>(filesBefore);
      removedFiles.removeAll(filesAfter);
      // The first and second table-metadata and the manifest-list from the second table metadata
      // got cleaned up.
      soft.assertThat(removedFiles)
          .allMatch(u -> u.location().endsWith(".json") || u.location().endsWith(".avro"));
    }
  }

  private Set<StorageUri> allFiles(IcebergFiles icebergFiles) throws NessieFileIOException {
    try (Stream<FileReference> list =
        icebergFiles.listRecursively(StorageUri.of(tempFile.toURI()))) {
      return list.map(FileReference::absolutePath).collect(Collectors.toCollection(TreeSet::new));
    }
  }
}
