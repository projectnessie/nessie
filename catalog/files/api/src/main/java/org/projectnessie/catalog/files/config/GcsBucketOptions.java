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
package org.projectnessie.catalog.files.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableGcsBucketOptions.class)
@JsonDeserialize(as = ImmutableGcsBucketOptions.class)
public interface GcsBucketOptions extends BucketOptions {

  /** Default value for {@link #authType()}, being {@link GcsAuthType#NONE}. */
  GcsAuthType DEFAULT_AUTH_TYPE = GcsAuthType.NONE;

  /**
   * The default endpoint override to use. The endpoint is almost always used for testing purposes.
   *
   * <p>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint
   * used for the Nessie server.
   */
  Optional<URI> host();

  /**
   * When using a specific endpoint, see {@code host}, and the endpoint URIs for the Nessie server
   * differ, you can specify the URI passed down to clients using this setting. Otherwise, clients
   * will receive the value from the {@code host} setting.
   */
  Optional<URI> externalHost();

  /** Optionally specify the user project (Google term). */
  Optional<String> userProject();

  /** The Google project ID. */
  Optional<String> projectId();

  /** The Google quota project ID. */
  Optional<String> quotaProjectId();

  /** The Google client lib token. */
  Optional<String> clientLibToken();

  /** The authentication type to use. If not set, the default is {@code NONE}. */
  Optional<GcsAuthType> authType();

  default GcsAuthType effectiveAuthType() {
    return authType().orElse(DEFAULT_AUTH_TYPE);
  }

  /**
   * Name of the key-secret containing the auth-credentials-JSON, this value is the name of the
   * credential to use, the actual credential is defined via secrets.
   */
  Optional<URI> authCredentialsJson();

  /**
   * Name of the token-secret containing the OAuth2 token, this value is the name of the credential
   * to use, the actual credential is defined via secrets.
   */
  Optional<URI> oauth2Token();

  Optional<GcsDownscopedCredentials> downscopedCredentials();

  /** The read chunk size in bytes. */
  OptionalInt readChunkSize();

  /** The write chunk size in bytes. */
  OptionalInt writeChunkSize();

  /** The delete batch size. */
  OptionalInt deleteBatchSize();

  /**
   * Name of the key-secret containing the customer-supplied AES256 key for blob encryption when
   * writing.
   *
   * @implNote This is currently unsupported.
   */
  Optional<URI> encryptionKey();

  /**
   * Name of the key-secret containing the customer-supplied AES256 key for blob decryption when
   * reading.
   *
   * @implNote This is currently unsupported.
   */
  Optional<URI> decryptionKey();

  enum GcsAuthType {
    NONE,
    USER,
    SERVICE_ACCOUNT,
    ACCESS_TOKEN,
    APPLICATION_DEFAULT
  }

  @Value.NonAttribute
  @JsonIgnore
  default GcsBucketOptions deepClone() {
    ImmutableGcsBucketOptions.Builder b = ImmutableGcsBucketOptions.builder().from(this);
    downscopedCredentials()
        .ifPresent(v -> b.downscopedCredentials(ImmutableGcsDownscopedCredentials.copyOf(v)));
    return b.build();
  }
}
