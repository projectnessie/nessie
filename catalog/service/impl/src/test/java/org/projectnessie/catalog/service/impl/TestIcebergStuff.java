/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.impl;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateCompressedMetadataForTable;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateCompressedMetadataForView;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateSimpleMetadata;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.generateSimpleMetadataForView;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures.objectWriterForPath;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.local.LocalObjectIO;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergGenerateFixtures;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.impl.TaskServiceMetrics;
import org.projectnessie.nessie.tasks.service.impl.TasksServiceImpl;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessiePersistCache;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessiePersistCache // should test w/ persist-cache to exercise custom obj type serialization
public class TestIcebergStuff {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  private ScheduledExecutorService executor;
  private TasksServiceImpl tasksService;

  @TempDir static Path tempDir;

  @BeforeEach
  public void setup() {
    executor = Executors.newScheduledThreadPool(2);
    JavaPoolTasksAsync tasksAsync = new JavaPoolTasksAsync(executor, Clock.systemUTC(), 1L);
    tasksService =
        new TasksServiceImpl(
            tasksAsync,
            mock(TaskServiceMetrics.class),
            TasksServiceConfig.tasksServiceConfig("t", 1L, 20L));
  }

  @AfterEach
  public void shutdown() throws Exception {
    tasksService.shutdown().toCompletableFuture().get(5, TimeUnit.MINUTES);
    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.MINUTES)).isTrue();
  }

  @Test
  public void retryImportSnapshot() {
    // TODO Let S3Object I/O throw a BackendThrottledException
  }

  @Test
  public void retryImportManifestList() {
    // TODO Let S3Object I/O throw a BackendThrottledException
  }

  @Test
  public void retryImportManifests() {
    // TODO Let S3Object I/O throw a BackendThrottledException
  }

  @ParameterizedTest
  @MethodSource("icebergTableImports")
  public void icebergTableImports(
      @SuppressWarnings("unused") String testName, String icebergTableMetadata) throws Exception {
    BackendExceptionMapper exceptionMapper = BackendExceptionMapper.builder().build();

    ObjectIO objectIO = new LocalObjectIO();
    IcebergStuff icebergStuff =
        new IcebergStuff(
            objectIO,
            persist,
            tasksService,
            new EntitySnapshotTaskBehavior(exceptionMapper, Duration.ofMillis(1)),
            executor);

    ObjId snapshotId = randomObjId();
    IcebergTable icebergTable =
        IcebergTable.of(icebergTableMetadata, 1, 1, 1, 1, randomUUID().toString());

    CompletionStage<NessieTableSnapshot> stage =
        icebergStuff.retrieveIcebergSnapshot(snapshotId, icebergTable);
    NessieTableSnapshot snapshot = stage.toCompletableFuture().get(1, TimeUnit.MINUTES);
    soft.assertThat(snapshot).isNotNull();
  }

  static Stream<Arguments> icebergTableImports() throws Exception {
    IcebergGenerateFixtures.ObjectWriter objectWriter = objectWriterForPath(tempDir);
    return Stream.of(
        arguments(
            "compressed table-metadata generic",
            generateCompressedMetadataForTable(objectWriter, 0)),
        arguments(
            "compressed table-metadata v1", generateCompressedMetadataForTable(objectWriter, 1)),
        arguments(
            "compressed table-metadata v2", generateCompressedMetadataForTable(objectWriter, 2)),
        arguments("simple table-metadata", generateSimpleMetadata(objectWriter, 2)));
  }

  @ParameterizedTest
  @MethodSource("icebergViewImports")
  public void icebergViewImports(
      @SuppressWarnings("unused") String testName, String icebergViewMetadata) throws Exception {
    BackendExceptionMapper exceptionMapper = BackendExceptionMapper.builder().build();

    ObjectIO objectIO = new LocalObjectIO();
    IcebergStuff icebergStuff =
        new IcebergStuff(
            objectIO,
            persist,
            tasksService,
            new EntitySnapshotTaskBehavior(exceptionMapper, Duration.ofMillis(1)),
            executor);

    ObjId snapshotId = randomObjId();
    IcebergView icebergView = IcebergView.of("1", icebergViewMetadata, 1, 1);

    CompletionStage<NessieViewSnapshot> stage =
        icebergStuff.retrieveIcebergSnapshot(snapshotId, icebergView);
    NessieViewSnapshot snapshot = stage.toCompletableFuture().get(1, TimeUnit.MINUTES);
    soft.assertThat(snapshot).isNotNull();
  }

  static Stream<Arguments> icebergViewImports() throws Exception {
    IcebergGenerateFixtures.ObjectWriter objectWriter = objectWriterForPath(tempDir);
    return Stream.of(
        arguments(
            "compressed view-metadata generic", generateCompressedMetadataForView(objectWriter, 0)),
        arguments(
            "compressed view-metadata v1", generateCompressedMetadataForView(objectWriter, 1)),
        arguments(
            "compressed view-metadata v2", generateCompressedMetadataForView(objectWriter, 2)),
        arguments("simple view-metadata", generateSimpleMetadataForView(objectWriter, 2)));
  }
}
