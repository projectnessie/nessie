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
package org.projectnessie.server.catalog.secrets;

import io.smallrye.config.SmallRyeConfig;
import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.secrets.AbstractMapBasedSecretsProvider;

public class SmallryeConfigSecretsProvider extends AbstractMapBasedSecretsProvider {
  private final SmallRyeConfig config;

  public SmallryeConfigSecretsProvider(SmallRyeConfig config) {
    this.config = config;
  }

  @Override
  protected Map<String, String> resolveSecret(@Nonnull URI name) {
    Optional<String> singleValue = config.getOptionalValue(name.toString(), String.class);
    if (singleValue.isPresent()) {
      return parseOrSingle(singleValue.get());
    }

    Optional<Map<String, String>> map =
        config.getOptionalValues(name.toString(), String.class, String.class);
    return map.orElse(null);
  }
}
