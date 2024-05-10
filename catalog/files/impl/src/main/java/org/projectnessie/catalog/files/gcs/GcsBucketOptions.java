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

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

public interface GcsBucketOptions {

  /**
   * The default endpoint override to use. The endpoint is almost always used for testing purposes.
   *
   * <p>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint
   * used for the Nessie server.
   */
  Optional<URI> host();

  /**
   * When using a {@linkplain #host() specific endpoint} and the endpoint URIs for the Nessie server
   * differ, you can specify the URI passed down to clients using this setting. Otherwise clients
   * will receive the value from the {@link #host()} setting.
   */
  Optional<URI> externalHost();

  /** Optionally specify the user project (Google term). */
  Optional<String> userProject();

  /** Override the default read timeout. */
  Optional<Duration> readTimeout();

  /** Override the default connection timeout. */
  Optional<Duration> connectTimeout();

  /** The Google project ID. */
  Optional<String> projectId();

  /** The Google quota project ID. */
  Optional<String> quotaProjectId();

  /** The Google client lib token. */
  Optional<String> clientLibToken();

  /** The authentication type to use. */
  Optional<GcsAuthType> authType();

  /**
   * Reference to the credentials-JSON. This value is the name of the credential to use, the actual
   * credential is defined via secrets.
   */
  Optional<String> authCredentialsJsonRef();

  /**
   * Reference to the OAuth2 token. This value is the name of the credential to use, the actual
   * credential is defined via secrets.
   */
  Optional<String> oauth2TokenRef();

  /** Timestamp when the OAuth2 token referenced via {@code oauth2-token-ref} expires. */
  Optional<Instant> oauth2TokenExpiresAt();

  /** Override the default maximum number of attempts. */
  OptionalInt maxAttempts();

  /** Override the default logical request timeout. */
  Optional<Duration> logicalTimeout();

  /** Override the default total timeout. */
  Optional<Duration> totalTimeout();

  /** Override the default initial retry delay. */
  Optional<Duration> initialRetryDelay();

  /** Override the default maximum retry delay. */
  Optional<Duration> maxRetryDelay();

  /** Override the default retry delay multiplier. */
  OptionalDouble retryDelayMultiplier();

  /** Override the default initial RPC timeout. */
  Optional<Duration> initialRpcTimeout();

  /** Override the default maximum RPC timeout. */
  Optional<Duration> maxRpcTimeout();

  /** Override the default RPC timeout multiplier. */
  OptionalDouble rpcTimeoutMultiplier();

  /** The read chunk size in bytes. */
  OptionalInt readChunkSize();

  /** The write chunk size in bytes. */
  OptionalInt writeChunkSize();

  /** The delete batch size. */
  OptionalInt deleteBatchSize();

  /**
   * Customer-supplied AES256 key for blob encryption when writing.
   *
   * @implNote This is currently unsupported.
   */
  Optional<String> encryptionKeyRef();

  /**
   * Customer-supplied AES256 key for blob decryption when reading.
   *
   * @implNote This is currently unsupported.
   */
  Optional<String> decryptionKeyRef();

  enum GcsAuthType {
    NONE,
    USER,
    SERVICE_ACCOUNT,
    ACCESS_TOKEN,
  }
}
