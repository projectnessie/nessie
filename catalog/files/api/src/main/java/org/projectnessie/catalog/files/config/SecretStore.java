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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableSecretStore.class)
@JsonDeserialize(as = ImmutableSecretStore.class)
public interface SecretStore {
  /**
   * Override to set the file path to a custom SSL key or trust store. {@code
   * nessie.catalog.service.s3.trust-store.type} and {@code
   * nessie.catalog.service.s3.trust-store.password} must be supplied as well when providing a
   * custom trust store.
   *
   * <p>When running in k8s or Docker, the path is local within the pod/container and must be
   * explicitly mounted.
   */
  Optional<Path> path();

  /**
   * Override to set the type of the custom SSL key or trust store specified in {@code
   * nessie.catalog.service.s3.trust-store.path}.
   *
   * <p>Supported types include {@code JKS}, {@code PKCS12}, and all key store types supported by
   * Java 17.
   */
  Optional<String> type();

  /**
   * Name of the key-secret containing the password for the custom SSL key or trust store specified
   * in {@code nessie.catalog.service.s3.trust-store.path}.
   */
  Optional<URI> password();
}
