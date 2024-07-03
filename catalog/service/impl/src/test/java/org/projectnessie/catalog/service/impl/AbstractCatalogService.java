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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.immutables.value.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3Config;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.files.s3.S3ProgrammaticOptions;
import org.projectnessie.catalog.files.s3.S3Sessions;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.nessie.combined.CombinedClientBuilder;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.impl.TaskServiceMetrics;
import org.projectnessie.nessie.tasks.service.impl.TasksServiceImpl;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessiePersistCache;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import software.amazon.awssdk.http.SdkHttpClient;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessiePersistCache // should test w/ persist-cache to exercise custom obj type serialization
public abstract class AbstractCatalogService {
  protected static final String BUCKET = "bucket";
  protected static final String WAREHOUSE = "mywarehouse";

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  protected ScheduledExecutorService executor;
  protected TasksServiceImpl tasksService;
  protected HeapStorageBucket heapStorageBucket;
  protected ObjectStorageMock.MockServer server;
  protected SdkHttpClient httpClient;
  protected CatalogServiceImpl catalogService;
  protected NessieApiV2 api;

  @BeforeEach
  public void createCatalogServiceInstance() {
    executor = Executors.newScheduledThreadPool(2);
    JavaPoolTasksAsync tasksAsync = new JavaPoolTasksAsync(executor, Clock.systemUTC(), 1L);
    tasksService =
        new TasksServiceImpl(
            tasksAsync,
            mock(TaskServiceMetrics.class),
            TasksServiceConfig.tasksServiceConfig("t", 1L, 20L));

    heapStorageBucket = HeapStorageBucket.newHeapStorageBucket();
    server =
        ObjectStorageMock.builder()
            .initAddress("localhost")
            .putBuckets(BUCKET, heapStorageBucket.bucket())
            .build()
            .start();
    S3Sessions sessions = new S3Sessions("foo", null);

    S3Config s3config = S3Config.builder().build();
    httpClient = S3Clients.apacheHttpClient(s3config);
    S3Options<S3BucketOptions> s3options =
        S3ProgrammaticOptions.builder()
            .defaultOptions(
                S3ProgrammaticOptions.S3PerBucketOptions.builder()
                    .accessKey(basicCredentials("foo", "bar"))
                    .region("eu-central-1")
                    .endpoint(server.getS3BaseUri())
                    .pathStyleAccess(true)
                    .build())
            .build();
    S3ClientSupplier clientSupplier =
        new S3ClientSupplier(
            httpClient,
            s3config,
            s3options,
            new SecretsProvider(
                (names) ->
                    names.stream()
                        .collect(Collectors.toMap(k -> k, k -> Map.of("secret", "secret")))),
            sessions);
    S3ObjectIO objectIO = new S3ObjectIO(clientSupplier, Clock.systemUTC());

    api = new CombinedClientBuilder().withPersist(persist).build(NessieApiV2.class);

    catalogService = new CatalogServiceImpl();
    catalogService.catalogConfig =
        ImmutableCatalogConfigForTest.builder()
            .defaultWarehouse(WAREHOUSE)
            .putWarehouses(
                WAREHOUSE,
                ImmutableWarehouseConfigForTest.builder()
                    .location("s3://" + BUCKET + "/foo/bar/baz/")
                    .build())
            .build();
    catalogService.tasksService = tasksService;
    catalogService.objectIO = objectIO;
    catalogService.persist = persist;
    catalogService.executor = executor;
    catalogService.nessieApi = api;
  }

  @AfterEach
  public void shutdown() throws Exception {
    try {
      server.close();
    } finally {
      try {
        httpClient.close();
      } finally {
        try {
          tasksService.shutdown().toCompletableFuture().get(5, TimeUnit.MINUTES);
        } finally {
          executor.shutdown();
          assertThat(executor.awaitTermination(5, TimeUnit.MINUTES)).isTrue();
        }
      }
    }
  }

  @Value.Immutable
  @SuppressWarnings("immutables:from")
  interface CatalogConfigForTest extends CatalogConfig {
    @Override
    Map<String, WarehouseConfigForTest> warehouses();

    @Override
    @Value.Check
    default void check() {
      CatalogConfig.super.check();
    }
  }

  @Value.Immutable
  interface WarehouseConfigForTest extends WarehouseConfig {}
}
