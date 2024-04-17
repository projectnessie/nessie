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
package org.projectnessie.client.auth.oauth2;

import java.time.Duration;
import java.time.Instant;

class OAuth2ClientUtils {

  static Duration shortestDelay(
      Instant now,
      Instant accessExpirationTime,
      Instant refreshExpirationTime,
      Duration refreshSafetyWindow,
      Duration minRefreshDelay) {
    Instant expirationTime =
        accessExpirationTime.isBefore(refreshExpirationTime)
            ? accessExpirationTime
            : refreshExpirationTime;
    Duration delay = Duration.between(now, expirationTime).minus(refreshSafetyWindow);
    if (delay.compareTo(minRefreshDelay) < 0) {
      delay = minRefreshDelay;
    }
    return delay;
  }

  static Instant tokenExpirationTime(Instant now, Token token, Duration defaultLifespan) {
    Instant expirationTime = null;
    if (token != null) {
      expirationTime = token.getExpirationTime();
      if (expirationTime == null) {
        try {
          JwtToken jwtToken = JwtToken.parse(token.getPayload());
          expirationTime = jwtToken.getExpirationTime();
        } catch (Exception ignored) {
          // fall through
        }
      }
    }
    if (expirationTime == null) {
      expirationTime = now.plus(defaultLifespan);
    }
    return expirationTime;
  }
}
