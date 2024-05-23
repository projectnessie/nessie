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
import io.smallrye.context.SmallRyeManagedExecutor;
import io.smallrye.context.SmallRyeThreadContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Clock;
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
import org.projectnessie.catalog.files.adls.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.adls.AdlsOptions;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions;
import org.projectnessie.catalog.files.gcs.GcsClients;
import org.projectnessie.catalog.files.gcs.GcsOptions;
import org.projectnessie.catalog.files.gcs.GcsStorageSupplier;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3ClientSupplier;
import org.projectnessie.catalog.files.s3.S3Clients;
import org.projectnessie.catalog.files.s3.S3CredentialsResolver;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.files.s3.S3Sessions;
import org.projectnessie.catalog.files.s3.S3SessionsManager;
import org.projectnessie.catalog.files.s3.S3Signer;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.nessie.combined.CombinedClientBuilder;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;
import org.projectnessie.nessie.tasks.async.wrapping.ThreadContextTasksAsync;
import org.projectnessie.nessie.tasks.service.TasksServiceConfig;
import org.projectnessie.nessie.tasks.service.impl.TasksServiceExecutor;
import org.projectnessie.quarkus.config.CatalogAdlsConfig;
import org.projectnessie.quarkus.config.CatalogGcsConfig;
import org.projectnessie.quarkus.config.CatalogS3Config;
import org.projectnessie.quarkus.config.CatalogServiceConfig;
import org.projectnessie.quarkus.config.QuarkusCatalogConfig;
import org.projectnessie.services.rest.RestV2ConfigResource;
import org.projectnessie.services.rest.RestV2TreeResource;
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

  void eagerCatalogConfigValidation(@Observes StartupEvent ev, CatalogConfig catalogConfig) {
    catalogConfig.check();
  }

  @Produces
  @Singleton
  @CatalogS3Client
  public SdkHttpClient sdkHttpClient(CatalogS3Config s3config) {
    return S3Clients.apacheHttpClient(s3config);
  }

  public void closeSdkHttpClient(@Disposes @CatalogS3Client SdkHttpClient client) {
    client.close();
  }

  @SuppressWarnings({"unchecked", "rawtypes", "UnnecessaryLocalVariable"})
  @Produces
  @Singleton
  public S3Options<S3BucketOptions> s3Options(CatalogS3Config s3Config) {
    S3Options opts = s3Config;
    S3Options<S3BucketOptions> r = opts;
    return r;
  }

  @SuppressWarnings({"unchecked", "rawtypes", "UnnecessaryLocalVariable"})
  @Produces
  @Singleton
  public GcsOptions<GcsBucketOptions> gcsOptions(CatalogGcsConfig gcsConfig) {
    GcsOptions opts = gcsConfig;
    GcsOptions<GcsBucketOptions> r = opts;
    return r;
  }

  @SuppressWarnings({"unchecked", "rawtypes", "UnnecessaryLocalVariable"})
  @Produces
  @Singleton
  public AdlsOptions<AdlsFileSystemOptions> adlsOptions(CatalogAdlsConfig adlsConfig) {
    AdlsOptions opts = adlsConfig;
    AdlsOptions<AdlsFileSystemOptions> r = opts;
    return r;
  }

  @Produces
  @Singleton
  public S3SessionsManager s3SessionsManager(
      S3Options<?> s3options,
      @CatalogS3Client SdkHttpClient sdkClient,
      MeterRegistry meterRegistry,
      SecretsProvider secretsProvider) {
    return new S3SessionsManager(s3options, sdkClient, meterRegistry, secretsProvider);
  }

  @Produces
  @Singleton
  public S3Sessions s3sessions(StoreConfig storeConfig, S3SessionsManager sessionsManager) {
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
  public SecretsProvider secretsProvider(QuarkusCatalogConfig config) {
    return new SecretsProvider() {
      @Override
      public String getSecret(String secret) {
        String value = config.secrets().get(secret);
        if (value != null) {
          return value;
        }
        throw new IllegalArgumentException(
            "Secret '"
                + secret
                + "' is not defined, configure it using the configuration option 'nessie.catalog.secrets."
                + secret
                + "'.");
      }
    };
  }

  @Produces
  @Singleton
  public HttpTransportFactory gcsHttpTransportFactory() {
    return GcsClients.buildSharedHttpTransportFactory();
  }

  @Produces
  @Singleton
  public HttpClient adlsHttpClient(CatalogAdlsConfig adlsConfig) {
    return AdlsClients.buildSharedHttpClient(adlsConfig);
  }

  @Produces
  @Singleton
  public ObjectIO objectIO(
      CatalogS3Config s3config,
      @CatalogS3Client SdkHttpClient sdkClient,
      CatalogAdlsConfig adlsConfig,
      HttpClient adlsHttpClient,
      CatalogGcsConfig gcsConfig,
      HttpTransportFactory gcsHttpTransportFactory,
      SecretsProvider secretsProvider,
      S3Sessions sessions) {
    S3ClientSupplier s3ClientSupplier =
        new S3ClientSupplier(sdkClient, s3config, s3config, secretsProvider, sessions);

    AdlsClientSupplier adlsClientSupplier =
        new AdlsClientSupplier(adlsHttpClient, adlsConfig, secretsProvider);

    GcsStorageSupplier gcsStorageSupplier =
        new GcsStorageSupplier(gcsHttpTransportFactory, gcsConfig, secretsProvider);

    return new ResolvingObjectIO(s3ClientSupplier, adlsClientSupplier, gcsStorageSupplier);
  }

  @Produces
  @Singleton
  public RequestSigner signer(
      CatalogS3Config s3config, SecretsProvider secretsProvider, S3Sessions s3sessions) {
    return new S3Signer(s3config, secretsProvider, s3sessions);
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

  @Produces
  @RequestScoped
  public NessieApiV2 nessieApiV2(
      RestV2ConfigResource configResource, RestV2TreeResource treeResource) {
    return new CombinedClientBuilder()
        .withConfigResource(configResource)
        .withTreeResource(treeResource)
        .build(NessieApiV2.class);
  }
}
