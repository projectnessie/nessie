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
package org.projectnessie.server.catalog;

import static java.time.Clock.systemUTC;

import com.azure.core.http.HttpClient;
import com.google.auth.http.HttpTransportFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.configuration.ConfigurationException;
import io.smallrye.context.SmallRyeManagedExecutor;
import io.smallrye.context.SmallRyeThreadContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.context.ThreadContext;
import org.projectnessie.catalog.files.ResolvingObjectIO;
import org.projectnessie.catalog.files.adls.AdlsClientSupplier;
import org.projectnessie.catalog.files.adls.AdlsClients;
import org.projectnessie.catalog.files.adls.AdlsExceptionMapper;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.config.AdlsConfig;
import org.projectnessie.catalog.files.config.GcsConfig;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3StsCache;
import org.projectnessie.catalog.files.gcs.GcsClients;
import org.projectnessie.catalog.files.gcs.GcsExceptionMapper;
import org.projectnessie.catalog.files.gcs.GcsStorageSupplier;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3CredentialsResolver;
import org.projectnessie.catalog.files.s3.S3ExceptionMapper;
import org.projectnessie.catalog.files.s3.S3Sessions;
import org.projectnessie.catalog.files.s3.S3Signer;
import org.projectnessie.catalog.files.s3.StsClientsPool;
import org.projectnessie.catalog.files.s3.StsCredentialsManager;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.SecretsValidation;
import org.projectnessie.catalog.service.config.SmallryeConfigs;
import org.projectnessie.catalog.service.impl.IcebergExceptionMapper;
import org.projectnessie.catalog.service.impl.IllegalArgumentExceptionMapper;
import org.projectnessie.catalog.service.impl.NessieExceptionMapper;
import org.projectnessie.catalog.service.impl.PreviousTaskExceptionMapper;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;
import org.projectnessie.nessie.tasks.async.wrapping.ThreadContextTasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.impl.TasksServiceExecutor;
import org.projectnessie.quarkus.config.CatalogServiceConfig;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * "Quick and dirty" producers providing connection to Nessie, a "storage" impl and object-store
 * I/O.
 */
public class CatalogProducers {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogProducers.class);

  void eagerCatalogConfigValidation(
      @Observes StartupEvent ev,
      LakehouseConfig lakehouseConfig,
      SmallryeConfigs smallryeConfigs,
      SecretsProvider secretsProvider) {
    SecretsValidation validation =
        SecretsValidation.builder().secretsProvider(secretsProvider).build();

    if (smallryeConfigs.validateSecrets()) {
      var failures = new ArrayList<SecretsValidation.SecretValidationFailure>();
      failures.addAll(validation.validateLakehouseConfig(lakehouseConfig));
      failures.addAll(validation.validateSmallryeConfigs(smallryeConfigs));

      if (!failures.isEmpty()) {
        for (SecretsValidation.SecretValidationFailure failure : failures) {
          if (failure.failure().isPresent()) {
            LOGGER.error(
                "The secret for '{}' referenced as '{}' could not be validated: {}",
                String.join(".", failure.propertyPath()),
                failure.uri(),
                failure.message(),
                failure.failure().get());
          } else {
            LOGGER.error(
                "The secret for '{}' referenced as '{}' could not be validated: {}",
                String.join(".", failure.propertyPath()),
                failure.uri(),
                failure.message());
          }
        }

        throw new ConfigurationException(
            "One or more secrets could not be validated. Please inspect the log messages above.");
      }
    }
  }

  @Produces
  @Singleton
  @CatalogS3Client
  public SdkHttpClient sdkHttpClient(S3Config s3config, SecretsProvider secretsProvider) {
    return S3Clients.apacheHttpClient(s3config, secretsProvider);
  }

  public void closeSdkHttpClient(@Disposes @CatalogS3Client SdkHttpClient client) {
    client.close();
  }

  @Produces
  @Singleton
  public StsClientsPool stsClientsPool(
      S3StsCache s3StsCache,
      @CatalogS3Client SdkHttpClient sdkClient,
      @Any Instance<MeterRegistry> meterRegistry) {
    return new StsClientsPool(
        s3StsCache, sdkClient, meterRegistry.isResolvable() ? meterRegistry.get() : null);
  }

  @Produces
  @Singleton
  public StsCredentialsManager s3SessionsManager(
      S3StsCache s3StsCache,
      StsClientsPool clientsPool,
      SecretsProvider secretsProvider,
      @Any Instance<MeterRegistry> meterRegistry) {
    return new StsCredentialsManager(
        s3StsCache,
        clientsPool,
        secretsProvider,
        meterRegistry.isResolvable() ? meterRegistry.get() : null);
  }

  @Produces
  @Singleton
  public S3Sessions s3sessions(StoreConfig storeConfig, StsCredentialsManager sessionsManager) {
    String repositoryId = storeConfig.repositoryId();
    return new S3Sessions(repositoryId, sessionsManager);
  }

  @Produces
  @Singleton
  public S3CredentialsResolver s3CredentialsResolver(S3Sessions sessions) {
    return new S3CredentialsResolver(Clock.systemUTC(), sessions);
  }

  @Produces
  @Singleton
  public HttpTransportFactory gcsHttpTransportFactory() {
    return GcsClients.buildSharedHttpTransportFactory();
  }

  @Produces
  @Singleton
  public HttpClient adlsHttpClient(AdlsConfig adlsConfig) {
    return AdlsClients.buildSharedHttpClient(adlsConfig);
  }

  @Produces
  @RequestScoped
  public S3ClientSupplier s3ClientSupplier(
      LakehouseConfig lakehouseConfig,
      @CatalogS3Client SdkHttpClient sdkClient,
      S3Sessions sessions,
      SecretsProvider secretsProvider) {
    return new S3ClientSupplier(sdkClient, lakehouseConfig.s3(), sessions, secretsProvider);
  }

  @Produces
  @RequestScoped
  public AdlsClientSupplier adlsClientSupplier(
      AdlsConfig adlsConfig,
      LakehouseConfig lakehouseConfig,
      HttpClient adlsHttpClient,
      SecretsProvider secretsProvider) {
    return new AdlsClientSupplier(
        adlsHttpClient, adlsConfig, lakehouseConfig.adls(), secretsProvider);
  }

  @Produces
  @RequestScoped
  public GcsStorageSupplier gcsStorageSupplier(
      GcsConfig gcsConfig,
      LakehouseConfig lakehouseConfig,
      HttpTransportFactory gcsHttpTransportFactory,
      SecretsProvider secretsProvider) {
    return new GcsStorageSupplier(
        gcsHttpTransportFactory, gcsConfig, lakehouseConfig.gcs(), secretsProvider);
  }

  @Produces
  @RequestScoped
  public ObjectIO objectIO(
      S3ClientSupplier s3ClientSupplier,
      S3CredentialsResolver s3CredentialsResolver,
      GcsStorageSupplier gcsStorageSupplier,
      AdlsClientSupplier adlsClientSupplier) {
    return new ResolvingObjectIO(
        s3ClientSupplier, s3CredentialsResolver, adlsClientSupplier, gcsStorageSupplier);
  }

  @Produces
  @RequestScoped
  public RequestSigner signer(
      LakehouseConfig lakehouseConfig, SecretsProvider secretsProvider, S3Sessions s3sessions) {
    return new S3Signer(lakehouseConfig.s3(), secretsProvider, s3sessions);
  }

  @Produces
  @Singleton
  public BackendExceptionMapper backendExceptionMapper() {
    return BackendExceptionMapper.builder()
        .addAnalyzer(PreviousTaskExceptionMapper.INSTANCE)
        .addAnalyzer(NessieExceptionMapper.INSTANCE)
        .addAnalyzer(IllegalArgumentExceptionMapper.INSTANCE)
        .addAnalyzer(IcebergExceptionMapper.INSTANCE)
        .addAnalyzer(AdlsExceptionMapper.INSTANCE)
        .addAnalyzer(GcsExceptionMapper.INSTANCE)
        .addAnalyzer(S3ExceptionMapper.INSTANCE)
        .build();
  }

  /**
   * Provides the {@link TasksAsync} instance backed by a thread-pool executor configured according
   * to {@link CatalogServiceConfig}, with thread-context propagation.
   */
  @Produces
  @Singleton
  @TasksServiceExecutor
  public TasksAsync tasksAsync(ThreadContext threadContext, CatalogServiceConfig config) {
    int maxThreads = config.tasksMaxThreads();
    if (maxThreads <= 0) {
      // Keep the max pool size between 2 and 16
      maxThreads = Math.min(16, Math.max(2, Runtime.getRuntime().availableProcessors()));
    }

    ScheduledThreadPoolExecutor executorService = buildScheduledExecutor();
    executorService.setKeepAliveTime(
        config.tasksThreadsKeepAlive().toMillis(), TimeUnit.MILLISECONDS);
    executorService.setMaximumPoolSize(maxThreads);

    LOGGER.debug(
        "Tasks handling configured with max {} threads, minimum delay of {}ms, race min/max of {}ms/{}ms.",
        maxThreads,
        config.tasksMinimumDelay().toMillis(),
        config.raceWaitMin(),
        config.raceWaitMax());

    TasksAsync base =
        new JavaPoolTasksAsync(executorService, systemUTC(), config.tasksMinimumDelay().toMillis());
    return new ThreadContextTasksAsync(base, threadContext);

    // Cannot use VertxTasksAsync :( - but using ThreadContext.contextual*() works fine.
    //    return new VertxTasksAsync(vertx, systemUTC(), 1L);
    //
    // With Vert.x Quarkus runs into this warning quite often:
    //   WARN  [io.qua.ope.run.QuarkusContextStorage] (executor-thread-1) Context in storage not the
    //     expected context, Scope.close was not called correctly. Details: OTel context before:
  }

  private static ScheduledThreadPoolExecutor buildScheduledExecutor() {
    ScheduledThreadPoolExecutor executorService =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactory() {
              private final ThreadGroup group = Thread.currentThread().getThreadGroup();
              private final AtomicInteger num = new AtomicInteger();

              @Override
              public Thread newThread(Runnable r) {
                return new Thread(group, r, "tasks-async-" + num.incrementAndGet());
              }
            });
    executorService.allowCoreThreadTimeOut(true);
    return executorService;
  }

  @Produces
  @Singleton
  public TasksServiceConfig tasksServiceConfig(CatalogServiceConfig config) {
    return TasksServiceConfig.tasksServiceConfig(
        "tasks", config.raceWaitMin().toMillis(), config.raceWaitMax().toMillis());
  }

  /** Provides the executor to run actual catalog import jobs, with thread-context propagation. */
  @Produces
  @Singleton
  @Named("import-jobs")
  public Executor importJobExecutor(ThreadContext threadContext, CatalogServiceConfig config) {
    ExecutorService executor =
        SmallRyeManagedExecutor.newThreadPoolExecutor(config.maxConcurrentImports(), -1);
    return new SmallRyeManagedExecutor(
        config.maxConcurrentImports(),
        -1,
        (SmallRyeThreadContext) threadContext,
        executor,
        "import-jobs");
  }
}
