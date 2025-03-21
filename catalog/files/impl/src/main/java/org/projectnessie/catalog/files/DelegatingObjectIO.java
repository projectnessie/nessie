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
package org.projectnessie.catalog.files;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.storage.uri.StorageUri;

public abstract class DelegatingObjectIO implements ObjectIO {
  protected abstract ObjectIO resolve(StorageUri uri);

  @Override
  public void ping(StorageUri uri) throws IOException {
    resolve(uri).ping(uri);
  }

  @Override
  public OutputStream writeObject(StorageUri uri) throws IOException {
    return resolve(uri).writeObject(uri);
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    return resolve(uri).readObject(uri);
  }

  @Override
  public Optional<String> canResolve(StorageUri uri) {
    try {
      ObjectIO resolved = resolve(uri);
      return resolved.canResolve(uri);
    } catch (IllegalArgumentException e) {
      return Optional.of(e.getMessage());
    }
  }

  @Override
  public void configureIcebergWarehouse(
      StorageUri warehouse,
      BiConsumer<String, String> defaultConfig,
      BiConsumer<String, String> configOverride) {
    resolve(warehouse).configureIcebergWarehouse(warehouse, defaultConfig, configOverride);
  }

  @Override
  public void configureIcebergTable(
      StorageLocations storageLocations,
      BiConsumer<String, String> config,
      Predicate<Duration> enableRequestSigning,
      boolean canDoCredentialsVending) {
    resolve(storageLocations.warehouseLocation())
        .configureIcebergTable(
            storageLocations, config, enableRequestSigning, canDoCredentialsVending);
  }

  @Override
  public void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties) {
    resolve(warehouse).trinoSampleConfig(warehouse, icebergConfig, properties);
  }
}
