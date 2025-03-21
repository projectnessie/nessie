/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.files.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.projectnessie.storage.uri.StorageUri;

public interface ObjectIO {
  String ICEBERG_FILE_IO_IMPL = "io-impl";
  String PYICEBERG_FILE_IO_IMPL = "py-io-impl";

  void ping(StorageUri uri) throws IOException;

  InputStream readObject(StorageUri uri) throws IOException;

  OutputStream writeObject(StorageUri uri) throws IOException;

  void deleteObjects(List<StorageUri> uris) throws IOException;

  /** Produces the Iceberg configuration options for "default config" and "config override". */
  void configureIcebergWarehouse(
      StorageUri warehouse,
      BiConsumer<String, String> defaultConfig,
      BiConsumer<String, String> configOverride);

  /**
   * Produces the Iceberg configuration for a table.
   *
   * @param storageLocations warehouse, readable and writeable locations
   * @param config configuration consumer
   * @param enableRequestSigning callback predicate that is called when request signing is possible
   *     for the bucket, must return whether request signing is effective
   * @param canDoCredentialsVending whether to configure credentials vending
   */
  void configureIcebergTable(
      StorageLocations storageLocations,
      BiConsumer<String, String> config,
      Predicate<Duration> enableRequestSigning,
      boolean canDoCredentialsVending);

  void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties);

  /**
   * Checks whether the given storage URI can be resolved.
   *
   * @return an empty optional, if the URI can be resolved, otherwise an error message.
   */
  Optional<String> canResolve(StorageUri uri);
}
