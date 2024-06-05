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
package org.projectnessie.catalog.secrets.spi;

import java.util.Collection;
import java.util.Map;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.TokenSecret;

/** SPI interface for actual secrets managers. */
public interface SecretsSupplier {
  /**
   * Resolve secrets.
   *
   * @param names names of the secrets to resolve
   * @return map of secret names to a map of key-value pairs representing the secret. The keys and
   *     -values depend on the type of secret. See {@link KeySecret#keySecret(Map)}, {@link
   *     BasicCredentials#basicCredentials(Map)}, {@link TokenSecret#tokenSecret(Map)}
   */
  Map<String, Map<String, String>> resolveSecrets(Collection<String> names);
}
