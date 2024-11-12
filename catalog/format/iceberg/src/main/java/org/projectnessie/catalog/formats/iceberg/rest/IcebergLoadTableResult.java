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
package org.projectnessie.catalog.formats.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;

/**
 * Load table result.
 *
 * <p>The following configurations should be respected by clients
 *
 * <h2>General Configurations</h2>
 *
 * <ul>
 *   <li>{@code token}: Authorization bearer token to use for table requests if OAuth2 security is
 *       enabled
 * </ul>
 *
 * <h2>AWS Configurations</h2>
 *
 * <p>The following configurations should be respected when working with tables stored in AWS S3
 *
 * <ul>
 *   <li>{@code client.region}: region to configure client for making requests to AWS
 *   <li>{@code s3.access-key-id}: id for credentials that provide access to the data in S3
 *   <li>{@code s3.secret-access-key}: secret for credentials that provide access to data in S3
 *   <li>{@code s3.session-token}: if present, this value should be used for as the session token
 *   <li>{@code s3.remote-signing-enabled}: if {@code true} remote signing should be performed as
 *       described in the {@code s3-signer-open-api.yaml} specification
 * </ul>
 *
 * <h2>Storage Credentials</h2>
 *
 * <p>Credentials for ADLS / GCS / S3 / ... are provided through the {@code storage-credentials}
 * field. Clients must first check whether the respective credentials exist in the {@code
 * storage-credentials} field before checking the {@code config} for credentials.
 */
public interface IcebergLoadTableResult extends IcebergBaseTableResult {

  @Nullable
  @Override
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String metadataLocation();

  @Override
  IcebergTableMetadata metadata();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> config();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Nullable
  List<IcebergStorageCredential> storageCredentials();

  @SuppressWarnings("unused")
  interface Builder<R extends IcebergLoadTableResult, B extends Builder<R, B>>
      extends IcebergBaseTableResult.Builder<R, B> {
    @CanIgnoreReturnValue
    B from(IcebergLoadTableResult instance);

    @CanIgnoreReturnValue
    B metadataLocation(@Nullable String metadataLocation);

    @CanIgnoreReturnValue
    B metadata(IcebergTableMetadata metadata);

    @CanIgnoreReturnValue
    B addStorageCredential(IcebergStorageCredential element);

    @CanIgnoreReturnValue
    B addStorageCredentials(IcebergStorageCredential... elements);

    @CanIgnoreReturnValue
    B storageCredentials(@Nullable Iterable<? extends IcebergStorageCredential> elements);

    @CanIgnoreReturnValue
    B addAllStorageCredentials(Iterable<? extends IcebergStorageCredential> elements);

    @CanIgnoreReturnValue
    B putConfig(String key, String value);

    @CanIgnoreReturnValue
    B putConfig(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    B config(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    B putAllConfig(Map<String, ? extends String> entries);

    R build();
  }
}
