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

import static org.projectnessie.catalog.secrets.SecretAttribute.secretAttribute;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.files.gcs.GcsProgrammaticOptions.GcsPerBucketOptions;
import org.projectnessie.catalog.secrets.SecretAttribute;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface GcsOptions<PER_BUCKET extends GcsBucketOptions> extends GcsBucketOptions {

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the defaults from the top-level GCS
   * settings.
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

  List<SecretAttribute<GcsBucketOptions, GcsPerBucketOptions.Builder, ?>> SECRET_ATTRIBUTES =
      ImmutableList.of(
          secretAttribute(
              "authCredentialsJson",
              SecretType.KEY,
              GcsBucketOptions::authCredentialsJson,
              GcsPerBucketOptions.Builder::authCredentialsJson),
          secretAttribute(
              "oauth2Token",
              SecretType.EXPIRING_TOKEN,
              GcsBucketOptions::oauth2Token,
              GcsPerBucketOptions.Builder::oauth2Token),
          secretAttribute(
              "encryptionKey",
              SecretType.KEY,
              GcsBucketOptions::encryptionKey,
              GcsPerBucketOptions.Builder::encryptionKey),
          secretAttribute(
              "decryptionKey",
              SecretType.KEY,
              GcsBucketOptions::decryptionKey,
              GcsPerBucketOptions.Builder::decryptionKey));

  default GcsBucketOptions resolveSecrets(
      String filesystemName, GcsBucketOptions specific, SecretsProvider secretsProvider) {
    GcsPerBucketOptions.Builder builder = GcsPerBucketOptions.builder().from(this);
    if (specific != null) {
      builder.from(specific);
    }

    return secretsProvider
        .applySecrets(
            builder, "object-stores.gcs", this, filesystemName, specific, SECRET_ATTRIBUTES)
        .build();
  }
}
