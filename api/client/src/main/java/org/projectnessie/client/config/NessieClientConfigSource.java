/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.config;

import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Helper functional interface adding a fallback-mechanism to chain multiple config-sources. */
@FunctionalInterface
public interface NessieClientConfigSource {
  @Nullable
  @jakarta.annotation.Nullable
  String getValue(@Nonnull @jakarta.annotation.Nonnull String key);

  @Nonnull
  @jakarta.annotation.Nonnull
  default NessieClientConfigSource fallbackTo(
      @Nonnull @jakarta.annotation.Nonnull NessieClientConfigSource fallback) {
    return k -> {
      String v = getValue(k);
      return v != null ? v : fallback.getValue(k);
    };
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  default Function<String, String> asFunction() {
    return this::getValue;
  }
}
