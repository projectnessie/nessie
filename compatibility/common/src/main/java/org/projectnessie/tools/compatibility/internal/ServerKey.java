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
package org.projectnessie.tools.compatibility.internal;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.api.Version;

/** Value object representing the version, type and configuration of a {@link NessieApi}. */
final class ServerKey {
  public static final String STORAGE_KIND_PROPERTY = "nessie.test.storage.kind";

  private final Version version;
  private final String storageName;
  private final StorageKind storageKind;
  private final Map<String, String> config;

  ServerKey(
      Version version, String storageName, StorageKind storageKind, Map<String, String> config) {
    this.version = Objects.requireNonNull(version);
    this.storageName = Objects.requireNonNull(storageName);
    this.storageKind = Objects.requireNonNull(storageKind);
    this.config = Objects.requireNonNull(config);
  }

  public static ServerKey forContext(
      ExtensionContext context,
      Version version,
      String storageName,
      Map<String, String> defaultConfig) {
    Map<String, String> config = new HashMap<>(defaultConfig);
    context
        .getTestClass()
        .ifPresent(
            instance ->
                findRepeatableAnnotations(instance, NessieServerProperty.class)
                    .forEach(prop -> config.put(prop.name(), prop.value())));

    String storeKindStr = config.get(STORAGE_KIND_PROPERTY);
    StorageKind kind = StorageKind.DATABASE_ADAPTER;
    if (storeKindStr != null) {
      kind = StorageKind.valueOf(storeKindStr);
    }

    return new ServerKey(version, storageName, kind, config);
  }

  Version getVersion() {
    return version;
  }

  String getStorageName() {
    return storageName;
  }

  public StorageKind getStorageKind() {
    return storageKind;
  }

  Map<String, String> getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerKey serverKey = (ServerKey) o;
    return Objects.equals(version, serverKey.version)
        && Objects.equals(storageName, serverKey.storageName)
        && Objects.equals(storageKind, serverKey.storageKind)
        && Objects.equals(config, serverKey.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, storageName, storageKind, config);
  }

  @Override
  public String toString() {
    return String.format(
        "server-%s-%s-%s-%s",
        getVersion(),
        storageName,
        storageKind,
        config.entrySet().stream()
            .map(e -> e.getKey() + '=' + e.getValue())
            .collect(Collectors.joining("_")));
  }

  public enum StorageKind {
    DATABASE_ADAPTER,
    PERSIST,
  }
}
