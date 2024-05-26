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

/** Represents a single-value "token". */
public interface TokenSecret extends Secret {
  @Nonnull
  String token();

  static TokenSecret tokenSecret(@Nonnull String token) {
    return new TokenSecret() {
      @Override
      @Nonnull
      public String token() {
        return token;
      }

      @Override
      public String toString() {
        return "<token-credentials>";
      }
    };
  }

  static TokenSecret tokenSecret(@Nonnull Map<String, String> value) {
    String token = value.get("token");
    if (token == null) {
      token = value.get("value");
    }
    return token != null ? TokenSecret.tokenSecret(token) : null;
  }
}
