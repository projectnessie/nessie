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
package org.projectnessie.operator.reconciler.nessie.resource.options;

import static org.apache.commons.lang3.StringUtils.uncapitalize;
import static org.projectnessie.operator.events.EventReason.InvalidVersionStoreConfig;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record VersionStoreOptions(
    @JsonPropertyDescription("The type of version store to use.")
        @Default("InMemory")
        @PrinterColumn(name = "Version Store")
        VersionStoreType type,
    @JsonPropertyDescription("Version store cache options.") @Default("{}")
        VersionStoreCacheOptions cache,
    @JsonPropertyDescription(
            "RocksDB options. Only required when using RocksDb version store type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        RocksDbOptions rocksDb,
    @JsonPropertyDescription(
            "DynamoDB options. Only required when using DynamoDb version store type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        DynamoDbOptions dynamoDb,
    @JsonPropertyDescription(
            "MongoDB options. Only required when using MongoDb version store type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        MongoDbOptions mongoDb,
    @JsonPropertyDescription(
            "Cassandra options. Only required when using Cassandra version store type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        CassandraOptions cassandra,
    @JsonPropertyDescription(
            "JDBC options. Only required when using Jdbc version store type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        JdbcOptions jdbc,
    @JsonPropertyDescription(
            "BigTable options. Only required when using BigTable version store type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        BigTableOptions bigTable) {

  public enum VersionStoreType {
    InMemory,
    RocksDb,
    DynamoDb,
    MongoDb,
    Cassandra,
    Jdbc,
    BigTable;

    @JsonIgnore
    public boolean supportsMultipleReplicas() {
      return this != InMemory && this != RocksDb;
    }

    @JsonIgnore
    public boolean requiresPvc() {
      return this == VersionStoreType.RocksDb;
    }
  }

  public VersionStoreOptions() {
    this(null, null, null, null, null, null, null, null);
  }

  public VersionStoreOptions {
    type = type != null ? type : VersionStoreType.InMemory;
    cache = cache != null ? cache : new VersionStoreCacheOptions();
  }

  public void validate() {
    for (VersionStoreType vst : VersionStoreType.values()) {
      if (vst != VersionStoreType.InMemory && vst == type && !isConfigured(vst)) {
        throw new InvalidSpecException(
            InvalidVersionStoreConfig,
            "Version store type is '%s', but spec.versionStore.%s is not configured."
                .formatted(type, uncapitalize(vst.name())));
      }
      if (vst != type && isConfigured(vst)) {
        throw new InvalidSpecException(
            InvalidVersionStoreConfig,
            "Version store type is '%s', but spec.versionStore.%s is configured."
                .formatted(type, uncapitalize(vst.name())));
      }
    }
    cache.validate();
    if (jdbc != null) {
      jdbc.validate();
    }
  }

  private boolean isConfigured(VersionStoreType type) {
    return switch (type) {
      case InMemory -> false;
      case RocksDb -> rocksDb != null;
      case DynamoDb -> dynamoDb != null;
      case MongoDb -> mongoDb != null;
      case Cassandra -> cassandra != null;
      case Jdbc -> jdbc != null;
      case BigTable -> bigTable != null;
    };
  }
}
