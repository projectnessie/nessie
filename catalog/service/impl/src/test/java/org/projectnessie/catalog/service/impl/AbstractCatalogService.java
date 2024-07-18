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
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec.addPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder.addSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID.assignUUID;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultPartitionSpec.setDefaultPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultSortOrder.setDefaultSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.UpgradeFormatVersion.upgradeFormatVersion;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement.AssertCreate.assertTableDoesNotExist;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.services.authz.AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.immutables.value.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3Config;
import org.projectnessie.catalog.files.s3.S3ObjectIO;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.files.s3.S3ProgrammaticOptions;
import org.projectnessie.catalog.files.s3.S3Sessions;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCatalogOperation;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.api.CatalogCommit;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.combined.CombinedClientBuilder;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.impl.TaskServiceMetrics;
import org.projectnessie.nessie.tasks.service.impl.TasksServiceImpl;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.InterceptingBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.ContentApiImpl;
import org.projectnessie.services.impl.DiffApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.rest.RestV2ConfigResource;
import org.projectnessie.services.rest.RestV2TreeResource;
import org.projectnessie.services.spi.ConfigService;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessiePersistCache;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
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
  protected InterceptingBucket interceptingBucket;
  protected ObjectStorageMock.MockServer objectStorageServer;
  protected SdkHttpClient httpClient;
  protected ObjectIO objectIO;
  protected CatalogServiceImpl catalogService;
  protected NessieApiV2 api;
  protected volatile Function<AccessContext, BatchAccessChecker> batchAccessCheckerFactory;

  protected ParsedReference commitSingle(Reference branch, ContentKey key)
      throws InterruptedException, ExecutionException, BaseNessieClientServerException {
    return commitMultiple(branch, key);
  }

  protected ParsedReference commitMultiple(Reference branch, ContentKey... keys)
      throws InterruptedException, ExecutionException, BaseNessieClientServerException {
    ParsedReference ref =
        parsedReference(branch.getName(), branch.getHash(), Reference.ReferenceType.BRANCH);
    CatalogCommit.Builder commit = CatalogCommit.builder();

    for (ContentKey key : keys) {

      commit
          .addOperations(
              IcebergCatalogOperation.builder()
                  .key(key)
                  .addUpdates(
                      assignUUID(UUID.randomUUID().toString()),
                      upgradeFormatVersion(2),
                      addSchema(IcebergSchema.builder().build(), 0),
                      setCurrentSchema(-1),
                      addPartitionSpec(IcebergPartitionSpec.UNPARTITIONED_SPEC),
                      setDefaultPartitionSpec(-1),
                      addSortOrder(IcebergSortOrder.UNSORTED_ORDER),
                      setDefaultSortOrder(-1))
                  .addRequirement(assertTableDoesNotExist())
                  .type(ICEBERG_TABLE)
                  .build())
          .build();
    }

    MultiTableUpdate update =
        catalogService.commit(ref, commit.build()).toCompletableFuture().get();
    branch = update.targetBranch();

    return parsedReference(branch.getName(), branch.getHash(), Reference.ReferenceType.BRANCH);
  }

  @BeforeEach
  public void createCatalogServiceInstance() {
    setupTasksService();

    setupNessieApi();

    setupObjectStorage();

    setupObjectIO();

    setupCatalogService();
  }

  private void setupTasksService() {
    executor = Executors.newScheduledThreadPool(2);
    JavaPoolTasksAsync tasksAsync = new JavaPoolTasksAsync(executor, Clock.systemUTC(), 1L);
    tasksService =
        new TasksServiceImpl(
            tasksAsync,
            mock(TaskServiceMetrics.class),
            TasksServiceConfig.tasksServiceConfig("t", 1L, 20L));
  }

  private void setupCatalogService() {
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

    catalogService.backendExceptionMapper = BackendExceptionMapper.builder().build();
  }

  private void setupObjectIO() {
    S3Sessions sessions = new S3Sessions("foo", null);
    S3Config s3config = S3Config.builder().build();
    httpClient = S3Clients.apacheHttpClient(s3config, new SecretsProvider(names -> Map.of()));
    S3Options<S3BucketOptions> s3options =
        S3ProgrammaticOptions.builder()
            .defaultOptions(
                S3ProgrammaticOptions.S3PerBucketOptions.builder()
                    .accessKey(basicCredentials("foo", "bar"))
                    .region("eu-central-1")
                    .endpoint(objectStorageServer.getS3BaseUri())
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
    objectIO = new S3ObjectIO(clientSupplier);
  }

  private void setupObjectStorage() {
    heapStorageBucket = HeapStorageBucket.newHeapStorageBucket();
    interceptingBucket = new InterceptingBucket(heapStorageBucket.bucket());
    objectStorageServer =
        ObjectStorageMock.builder()
            .initAddress("localhost")
            .putBuckets(BUCKET, interceptingBucket)
            .build()
            .start();
  }

  private void setupNessieApi() {
    batchAccessCheckerFactory = accessContext -> NOOP_ACCESS_CHECKER;

    ServerConfig config =
        new ServerConfig() {
          @Override
          public String getDefaultBranch() {
            return "main";
          }

          @Override
          public boolean sendStacktraceToClient() {
            return true;
          }
        };
    VersionStore versionStore = new VersionStoreImpl(persist);
    Authorizer authorizer = context -> batchAccessCheckerFactory.apply(context);
    AccessContext accessContext = () -> () -> null;
    ConfigService configService =
        new ConfigApiImpl(config, versionStore, authorizer, accessContext, 2);
    TreeService treeService = new TreeApiImpl(config, versionStore, authorizer, accessContext);
    ContentService contentService =
        new ContentApiImpl(config, versionStore, authorizer, accessContext);
    DiffService diffService = new DiffApiImpl(config, versionStore, authorizer, accessContext);

    RestV2TreeResource treeResource =
        new RestV2TreeResource(configService, treeService, contentService, diffService);
    RestV2ConfigResource configResource =
        new RestV2ConfigResource(config, versionStore, authorizer, accessContext);
    api =
        new CombinedClientBuilder()
            .withTreeResource(treeResource)
            .withConfigResource(configResource)
            .build(NessieApiV2.class);
  }

  @AfterEach
  public void shutdown() throws Exception {
    try {
      api.close();
    } finally {
      try {
        objectStorageServer.close();
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
