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
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.TokenSecret;

public interface GcsBucketOptions {

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
   * Auth-credentials-JSON, this value is the name of the credential to use, the actual credential
   * is defined via secrets.
   */
  Optional<KeySecret> authCredentialsJson();

  /**
   * OAuth2 token, this value is the name of the credential to use, the actual credential is defined
   * via secrets.
   */
  Optional<TokenSecret> oauth2Token();

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
  Optional<KeySecret> encryptionKey();

  /**
   * Customer-supplied AES256 key for blob decryption when reading.
   *
   * @implNote This is currently unsupported.
   */
  Optional<KeySecret> decryptionKey();

  enum GcsAuthType {
    NONE,
    USER,
    SERVICE_ACCOUNT,
    ACCESS_TOKEN,
    APPLICATION_DEFAULT
  }
}
