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
package org.projectnessie.operator.reconciler.nessie.dependent;

import static org.projectnessie.operator.events.EventReason.CreatingConfigMap;
import static org.projectnessie.operator.events.EventReason.EnvVarOverwritten;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.NessieSpec.LogLevel;
import org.projectnessie.operator.reconciler.nessie.resource.options.AuthorizationOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.BigTableOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.CassandraOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.DynamoDbOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.JdbcOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.MongoDbOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.VersionStoreCacheOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.VersionStoreOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.VersionStoreOptions.VersionStoreType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class ConfigMapDependent extends CRUDKubernetesDependentResource<ConfigMap, Nessie> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMapDependent.class);

  private static final long MIB = 1024L * 1024L;

  public ConfigMapDependent() {
    super(ConfigMap.class);
  }

  @Override
  public ConfigMap create(ConfigMap desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating config-map {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie, CreatingConfigMap, "Creating config-map %s", desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  @Override
  public ConfigMap desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    return new ConfigMapBuilder()
        .withMetadata(helper.metaBuilder(nessie).build())
        .withData(collectConfig(nessie, context))
        .build();
  }

  private static Map<String, String> collectConfig(Nessie nessie, Context<Nessie> context) {
    Map<String, String> config = new TreeMap<>();
    configureLogLevel(nessie, config);
    configureVersionStore(nessie, config);
    configureAuthentication(nessie, config);
    configureAuthorization(nessie, config);
    configureTelemetry(nessie, config);
    configureAdvancedConfig(nessie, config);
    configureJvmOptions(nessie, config);
    configureDebug(nessie, config);
    configureExtraEnv(nessie, config, context);
    return config;
  }

  private static void configureLogLevel(Nessie nessie, Map<String, String> config) {
    LogLevel logLevel = nessie.getSpec().logLevel();
    if (logLevel.compareTo(LogLevel.INFO) < 0) {
      config.put("QUARKUS_LOG_LEVEL", logLevel.name());
      config.put("QUARKUS_LOG_CONSOLE_LEVEL", logLevel.name());
      config.put("QUARKUS_LOG_FILE_LEVEL", logLevel.name());
      config.put("QUARKUS_LOG_MIN_LEVEL", logLevel.name());
    }
  }

  private static void configureAuthentication(Nessie nessie, Map<String, String> config) {
    if (nessie.getSpec().authentication().enabled()) {
      config.put("NESSIE_SERVER_AUTHENTICATION_ENABLED", "true");
      String oidcAuthServerUrl = nessie.getSpec().authentication().oidcAuthServerUrl();
      config.put("QUARKUS_OIDC_AUTH_SERVER_URL", oidcAuthServerUrl);
      String oidcClientId = nessie.getSpec().authentication().oidcClientId();
      config.put("QUARKUS_OIDC_CLIENT_ID", oidcClientId);
    } else {
      config.put("QUARKUS_OIDC_TENANT_ENABLED", "false");
    }
  }

  private static void configureAuthorization(Nessie nessie, Map<String, String> config) {
    AuthorizationOptions authorization = nessie.getSpec().authorization();
    if (authorization.enabled()) {
      config.put("NESSIE_SERVER_AUTHORIZATION_ENABLED", "true");
      for (Map.Entry<String, String> entry : authorization.rules().entrySet()) {
        config.put(
            "NESSIE_SERVER_AUTHORIZATION_RULES_" + entry.getKey().toUpperCase(), entry.getValue());
      }
    }
  }

  private static void configureTelemetry(Nessie nessie, Map<String, String> config) {
    if (nessie.getSpec().telemetry().enabled()) {
      String endpoint = nessie.getSpec().telemetry().endpoint();
      config.put("QUARKUS_OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint);
      Map<String, String> attributes =
          new LinkedHashMap<>(nessie.getSpec().telemetry().attributes());
      attributes.putIfAbsent("service.name", nessie.getMetadata().getName());
      String attributesStr =
          attributes.entrySet().stream()
              .map(e -> e.getKey() + "=" + e.getValue())
              .reduce((a, b) -> a + "," + b)
              .orElse("");
      config.put("QUARKUS_OTEL_RESOURCE_ATTRIBUTES", attributesStr);
      String sample = nessie.getSpec().telemetry().sample();
      if (sample != null && !sample.isEmpty()) {
        switch (sample) {
          case "all" -> config.put("QUARKUS_OTEL_TRACES_SAMPLER", "parentbased_always_on");
          case "none" -> config.put("QUARKUS_OTEL_TRACES_SAMPLER", "parentbased_always_off");
          default -> {
            config.put("QUARKUS_OTEL_TRACES_SAMPLER", "parentbased_traceidratio");
            config.put("QUARKUS_OTEL_TRACES_SAMPLER_ARG", sample);
          }
        }
      }
    } else {
      config.put("QUARKUS_OTEL_SDK_DISABLED", "true");
    }
  }

  private static void configureVersionStore(Nessie nessie, Map<String, String> config) {
    VersionStoreOptions versionStore = nessie.getSpec().versionStore();
    configureVersionStoreCache(nessie, config);
    VersionStoreType type = versionStore.type();
    switch (type) {
      case InMemory -> {}
      case RocksDb -> configureRocks(config);
      case Jdbc -> configureJdbc(nessie, config);
      case BigTable -> configureBigTable(nessie, config);
      case MongoDb -> configureMongo(nessie, config);
      case Cassandra -> configureCassandra(nessie, config);
      case DynamoDb -> configureDynamo(nessie, config);
      default -> throw new AssertionError("Unexpected version store type: " + type);
    }
  }

  private static void configureVersionStoreCache(Nessie nessie, Map<String, String> config) {
    VersionStoreCacheOptions cache = nessie.getSpec().versionStore().cache();
    if (cache.enabled()) {
      if (cache.fixedSize() != null) {
        long mb = cache.fixedSize().getNumericalAmount().longValue() / MIB;
        config.put("NESSIE_VERSION_STORE_PERSIST_CACHE_CAPACITY_MB", String.valueOf(mb));
      } else {
        if (!cache.heapPercentage().equals(VersionStoreCacheOptions.DEFAULT_HEAP_PERCENTAGE)) {
          double hf = cache.heapPercentage().getNumericalAmount().doubleValue();
          config.put(
              "NESSIE_VERSION_STORE_PERSIST_CACHE_CAPACITY_FRACTION_OF_HEAP", String.valueOf(hf));
        }
        if (!cache.minSize().equals(VersionStoreCacheOptions.DEFAULT_MIN_SIZE)) {
          long ms = cache.minSize().getNumericalAmount().longValue() / MIB;
          config.put(
              "NESSIE_VERSION_STORE_PERSIST_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB",
              String.valueOf(ms));
        }
        if (!cache.minFreeHeap().equals(VersionStoreCacheOptions.DEFAULT_MIN_FREE_HEAP)) {
          long mfh = cache.minFreeHeap().getNumericalAmount().longValue() / MIB;
          config.put(
              "NESSIE_VERSION_STORE_PERSIST_CACHE_CAPACITY_FRACTION_ADJUST_MB",
              String.valueOf(mfh));
        }
      }
    } else {
      config.put("NESSIE_VERSION_STORE_PERSIST_CACHE_CAPACITY_MB", "0");
    }
  }

  private static void configureRocks(Map<String, String> config) {
    config.put("NESSIE_VERSION_STORE_TYPE", "ROCKSDB");
    config.put("NESSIE_VERSION_STORE_PERSIST_ROCKS_DATABASE_PATH", "/rocks-nessie");
  }

  private static void configureJdbc(Nessie nessie, Map<String, String> config) {
    JdbcOptions jdbc = Objects.requireNonNull(nessie.getSpec().versionStore().jdbc());
    config.put("NESSIE_VERSION_STORE_TYPE", "JDBC");
    config.put("QUARKUS_DATASOURCE_JDBC_URL", jdbc.url());
    if (jdbc.catalog() != null) {
      config.put("NESSIE_VERSION_STORE_PERSIST_JDBC_CATALOG", jdbc.catalog());
    }
    if (jdbc.schema() != null) {
      config.put("NESSIE_VERSION_STORE_PERSIST_JDBC_SCHEMA", jdbc.schema());
    }
  }

  private static void configureBigTable(Nessie nessie, Map<String, String> config) {
    BigTableOptions bigTable = Objects.requireNonNull(nessie.getSpec().versionStore().bigTable());
    config.put("NESSIE_VERSION_STORE_TYPE", "BIGTABLE");
    config.put("QUARKUS_GOOGLE_CLOUD_PROJECT_ID", bigTable.projectId());
    config.put("NESSIE_VERSION_STORE_PERSIST_BIGTABLE_INSTANCE_ID", bigTable.instanceId());
    config.put("NESSIE_VERSION_STORE_PERSIST_BIGTABLE_APP_PROFILE_ID", bigTable.appProfileId());
    if (bigTable.credentials() != null) {
      config.put("GOOGLE_APPLICATION_CREDENTIALS", "/bigtable-nessie/sa_credentials.json");
    }
  }

  private static void configureMongo(Nessie nessie, Map<String, String> config) {
    MongoDbOptions mongoDb = Objects.requireNonNull(nessie.getSpec().versionStore().mongoDb());
    config.put("NESSIE_VERSION_STORE_TYPE", "MONGODB");
    config.put("QUARKUS_MONGODB_CONNECTION_STRING", mongoDb.connectionString());
    config.put("QUARKUS_MONGODB_DATABASE", mongoDb.databaseName());
  }

  private static void configureCassandra(Nessie nessie, Map<String, String> config) {
    CassandraOptions cassandra =
        Objects.requireNonNull(nessie.getSpec().versionStore().cassandra());
    config.put("NESSIE_VERSION_STORE_TYPE", "CASSANDRA");
    config.put("QUARKUS_CASSANDRA_KEYSPACE", cassandra.keyspace());
    config.put(
        "QUARKUS_CASSANDRA_CONTACT_POINTS",
        cassandra.contactPoints().stream().reduce((a, b) -> a + "," + b).orElse(""));
    config.put("QUARKUS_CASSANDRA_LOCAL_DATACENTER", cassandra.localDatacenter());
  }

  private static void configureDynamo(Nessie nessie, Map<String, String> config) {
    DynamoDbOptions dynamoDb = Objects.requireNonNull(nessie.getSpec().versionStore().dynamoDb());
    config.put("NESSIE_VERSION_STORE_TYPE", "DYNAMODB");
    config.put("AWS_REGION", dynamoDb.region());
  }

  private static void configureAdvancedConfig(Nessie nessie, Map<String, String> config) {
    JsonNode advancedConfig = nessie.getSpec().advancedConfig();
    if (advancedConfig != null && !advancedConfig.isEmpty()) {
      applyAdvancedConfig(config, advancedConfig, "");
    }
  }

  private static void applyAdvancedConfig(
      Map<String, String> config, JsonNode configNode, String prefix) {
    for (Map.Entry<String, JsonNode> entry : configNode.properties()) {
      String key = prefix + entry.getKey();
      JsonNode value = entry.getValue();
      if (value.isObject()) {
        applyAdvancedConfig(config, value, key + ".");
      } else {
        assert value.isValueNode(); // already validated
        String envVarName = toEnvVarName(key);
        config.put(envVarName, value.asText());
      }
    }
  }

  private static String toEnvVarName(String key) {
    return key.toUpperCase().replace("\"", "_").replace(".", "_").replace("-", "_");
  }

  private static void configureJvmOptions(Nessie nessie, Map<String, String> config) {
    nessie.getSpec().jvmOptions().stream()
        .map(Objects::toString)
        .reduce((a, b) -> a + " " + b)
        .ifPresent(s -> config.put("JAVA_OPTS_APPEND", s));
  }

  private static void configureDebug(Nessie nessie, Map<String, String> config) {
    if (nessie.getSpec().remoteDebug().enabled()) {
      config.put("JAVA_DEBUG", "true");
      config.put("JAVA_DEBUG_PORT", String.valueOf(nessie.getSpec().remoteDebug().port()));
    }
  }

  private static void configureExtraEnv(
      Nessie nessie, Map<String, String> config, Context<Nessie> context) {
    if (nessie.getSpec().extraEnv() != null) {
      for (EnvVar e : nessie.getSpec().extraEnv()) {
        if (e.getValueFrom() == null) {
          String previous = config.put(e.getName(), e.getValue());
          if (previous != null) {
            EventService.retrieveFromContext(context)
                .fireEvent(
                    nessie,
                    EnvVarOverwritten,
                    "Overwriting existing environment variable %s; old value: %s; new value: %s",
                    e.getName(),
                    previous,
                    e.getValue());
          }
        }
      }
    }
  }
}
