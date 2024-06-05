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
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/** Represents a "token" with an optional expiration. */
public interface ExpiringTokenSecret extends Secret {

  /**
   * Name of the map-key used in the argument to {@link #expiringTokenSecret(Map)} for the value for
   * {@link #token()}.
   */
  String JSON_TOKEN = "token";

  /**
   * Name of the map-key used in the argument to {@link #expiringTokenSecret(Map)} for the value for
   * {@link #expiresAt()} parsed using {@link Instant#parse(CharSequence)}.
   */
  String JSON_EXPIRES_AT = "expiresAt";

  @Nonnull
  String token();

  @Nonnull
  Optional<Instant> expiresAt();

  static ExpiringTokenSecret expiringTokenSecret(
      @Nonnull String token, @Nullable Instant expiresAt) {
    return new ExpiringTokenSecret() {
      @Override
      @Nonnull
      public String token() {
        return token;
      }

      @Override
      @Nonnull
      public Optional<Instant> expiresAt() {
        return Optional.ofNullable(expiresAt);
      }

      @Override
      public String toString() {
        return "<token-credentials>";
      }
    };
  }

  /**
   * Builds an {@linkplain ExpiringTokenSecret expiring token} from its map representation.
   *
   * <p>{@link #token()} is retrieved from the key {@code key}, or if not present from the key
   * {@code value}. {@linkplain #expiresAt()} is retrieved from the key {@code expiresAt} using
   * {@link Instant#parse(CharSequence)} to convert it from the string representation.
   */
  static ExpiringTokenSecret expiringTokenSecret(@Nonnull Map<String, String> value) {
    String name = value.get(JSON_TOKEN);
    if (name == null) {
      name = value.get("value");
    }
    if (name == null) {
      return null;
    }
    String e = value.get(JSON_EXPIRES_AT);
    Instant expiresAt = null;
    if (e != null) {
      try {
        expiresAt = Instant.parse(e);
      } catch (Exception ignore) {
        // ignore
      }
    }
    return expiringTokenSecret(name, expiresAt);
  }
}
