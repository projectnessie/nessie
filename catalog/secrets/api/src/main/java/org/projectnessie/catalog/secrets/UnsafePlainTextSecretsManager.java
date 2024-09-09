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
package org.projectnessie.catalog.secrets;

import jakarta.annotation.Nonnull;
import java.util.Map;

public class UnsafePlainTextSecretsManager extends AbstractMapBasedSecretsManager {
  private final Map<String, Map<String, String>> unsafeSecrets;

  private UnsafePlainTextSecretsManager(Map<String, Map<String, String>> unsafeSecrets) {
    this.unsafeSecrets = unsafeSecrets;
  }

  public static SecretsManager unsafePlainTextSecretsProvider(
      Map<String, Map<String, String>> unsafeSecrets) {
    return new UnsafePlainTextSecretsManager(unsafeSecrets);
  }

  @Override
  protected Map<String, String> resolveSecret(@Nonnull String name) {
    return unsafeSecrets.get(name);
  }
}
