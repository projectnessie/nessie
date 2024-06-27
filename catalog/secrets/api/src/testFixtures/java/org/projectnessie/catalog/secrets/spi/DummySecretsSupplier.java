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

import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;

public class DummySecretsSupplier implements SecretsSupplier {
  @Override
  public Map<String, Secret> resolveSecrets(Map<String, SecretType> toResolve) {
    return toResolve.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    e.getValue()
                        .fromValueMap(Map.of("value", "key", "name", "name", "secret", "secret"))));
  }
}
