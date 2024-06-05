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

/** Represents a name + secret pair. */
public interface BasicCredentials extends Secret {
  @Nonnull
  String name();

  @Nonnull
  String secret();

  static BasicCredentials basicCredentials(@Nonnull String name, @Nonnull String secret) {
    return new BasicCredentials() {
      @Override
      @Nonnull
      public String name() {
        return name;
      }

      @Override
      @Nonnull
      public String secret() {
        return secret;
      }

      @Override
      public String toString() {
        return "<basic-credentials>";
      }
    };
  }

  /**
   * Builds a {@linkplain BasicCredentials basic credentials} from its map representation.
   *
   * <p>{@link #name()} is retrieved from the key {@code name}. {@linkplain #secret()} is retrieved
   * from the key {@code secret}.
   */
  static BasicCredentials basicCredentials(@Nonnull Map<String, String> value) {
    String name = value.get("name");
    if (name == null) {
      return null;
    }
    String secret = value.get("secret");
    if (secret == null) {
      return null;
    }
    return basicCredentials(name, secret);
  }
}
