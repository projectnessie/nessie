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

/** Represents a single-value "key". */
public interface KeySecret extends Secret {

  /**
   * Name of the map-key used in the argument to {@link #keySecret(Map)} for the value for {@link
   * #key()}.
   */
  String JSON_KEY = "key";

  @Nonnull
  String key();

  @Override
  default Map<String, String> asMap() {
    return Map.of(JSON_KEY, key());
  }

  static KeySecret keySecret(@Nonnull String key) {
    return new KeySecret() {
      @Override
      @Nonnull
      public String key() {
        return key;
      }

      @Override
      public String toString() {
        return "<key-credentials>";
      }
    };
  }

  /**
   * Builds a {@linkplain KeySecret key secret} from its map representation.
   *
   * <p>The secret is retrieved from the key {@code key}, or if not present from the key {@code
   * value}.
   */
  static KeySecret keySecret(@Nonnull Map<String, String> value) {
    String key = value.get(JSON_KEY);
    if (key == null) {
      key = value.get("value");
    }
    return key != null ? KeySecret.keySecret(key) : null;
  }
}
