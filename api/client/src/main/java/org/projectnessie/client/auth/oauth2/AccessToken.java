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
package org.projectnessie.client.auth.oauth2;

import jakarta.annotation.Nullable;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
public interface AccessToken extends Token {

  /** The type of the token issued, typically "Bearer". */
  String getTokenType();

  static AccessToken of(String payload, String tokenType, @Nullable Instant expirationTime) {
    return ImmutableAccessToken.builder()
        .payload(payload)
        .tokenType(tokenType)
        .expirationTime(expirationTime)
        .build();
  }
}
