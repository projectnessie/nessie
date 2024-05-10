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
package org.projectnessie.catalog.files.gcs;

import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.storage.Storage;
import java.util.Optional;
import org.projectnessie.catalog.files.secrets.SecretsProvider;

public final class GcsStorageSupplier {
  private final HttpTransportFactory httpTransportFactory;
  private final GcsOptions<?> gcsOptions;
  private final SecretsProvider secretsProvider;

  public GcsStorageSupplier(
      HttpTransportFactory httpTransportFactory,
      GcsOptions<?> gcsOptions,
      SecretsProvider secretsProvider) {
    this.httpTransportFactory = httpTransportFactory;
    this.gcsOptions = gcsOptions;
    this.secretsProvider = secretsProvider;
  }

  public SecretsProvider secretsProvider() {
    return secretsProvider;
  }

  public GcsBucketOptions bucketOptions(GcsLocation location) {
    return gcsOptions.effectiveOptionsForBucket(Optional.of(location.bucket()));
  }

  public Storage forLocation(GcsBucketOptions bucketOptions) {
    return GcsClients.buildStorage(bucketOptions, httpTransportFactory, secretsProvider);
  }
}
