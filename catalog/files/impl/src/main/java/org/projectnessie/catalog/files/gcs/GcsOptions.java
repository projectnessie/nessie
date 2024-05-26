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

import static org.projectnessie.catalog.files.secrets.SecretsHelper.Specializeable.specializable;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.files.gcs.GcsProgrammaticOptions.GcsPerBucketOptions;
import org.projectnessie.catalog.files.secrets.SecretsHelper;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface GcsOptions<PER_BUCKET extends GcsBucketOptions> extends GcsBucketOptions {

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the values from {@link GcsOptions}.
   */
  @ConfigPropertyName("bucket-name")
  Map<String, PER_BUCKET> buckets();

  default GcsBucketOptions effectiveOptionsForBucket(
      Optional<String> bucketName, SecretsProvider secretsProvider) {
    if (bucketName.isEmpty()) {
      return resolveSecrets(null, null, secretsProvider);
    }
    String name = bucketName.get();
    GcsBucketOptions perBucket = buckets().get(name);
    if (perBucket == null) {
      return resolveSecrets(name, null, secretsProvider);
    }

    return resolveSecrets(name, perBucket, secretsProvider);
  }

  List<SecretsHelper.Specializeable<GcsBucketOptions, GcsPerBucketOptions.Builder>> SECRETS =
      ImmutableList.of(
          specializable(
              "authCredentialsJson",
              GcsBucketOptions::authCredentialsJson,
              GcsPerBucketOptions.Builder::authCredentialsJson),
          specializable(
              "oauth2Token",
              GcsBucketOptions::oauth2Token,
              GcsPerBucketOptions.Builder::oauth2Token),
          specializable(
              "encryptionKey",
              GcsBucketOptions::encryptionKey,
              GcsPerBucketOptions.Builder::encryptionKey),
          specializable(
              "decryptionKey",
              GcsBucketOptions::decryptionKey,
              GcsPerBucketOptions.Builder::decryptionKey));

  default GcsBucketOptions resolveSecrets(
      String filesystemName, GcsBucketOptions specific, SecretsProvider secretsProvider) {
    GcsPerBucketOptions.Builder builder = GcsPerBucketOptions.builder().from(this);
    if (specific != null) {
      builder.from(specific);
    }

    SecretsHelper.resolveSecrets(
        secretsProvider, "object-stores.gcs", builder, this, filesystemName, specific, SECRETS);

    return builder.build();
  }
}
