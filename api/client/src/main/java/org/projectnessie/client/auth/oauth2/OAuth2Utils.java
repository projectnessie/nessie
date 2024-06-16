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

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.Status;

class OAuth2Utils {

  private static final Random RANDOM = new SecureRandom();

  /**
   * Common locations for OpenID provider metadata.
   *
   * @see <a
   *     href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata">OpenID
   *     Connect Discovery 1.0</a>
   * @see <a href="https://tools.ietf.org/html/rfc8414#section-5">RFC 8414 Section 5</a>
   */
  private static final String[] WELL_KNOWN_PATHS =
      new String[] {".well-known/openid-configuration", ".well-known/oauth-authorization-server"};

  static String randomAlphaNumString(int length) {
    return RANDOM
        .ints('0', 'z' + 1)
        .filter(i -> (i <= '9') || (i >= 'A' && i <= 'Z') || (i >= 'a'))
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  public static JsonNode fetchOpenIdProviderMetadata(HttpClient httpClient, URI issuerUrl) {
    List<Exception> failures = null;
    for (String path : WELL_KNOWN_PATHS) {
      try {
        HttpResponse response = httpClient.newRequest(issuerUrl).path(path).get();
        Status status = response.getStatus();
        if (status != Status.OK) {
          throw new HttpClientResponseException(
              "OpenID provider metadata request returned status code " + status.getCode(), status);
        }
        JsonNode data = response.readEntity(JsonNode.class);
        if (!data.has("issuer") || !data.has("authorization_endpoint")) {
          throw new HttpClientException("Invalid OpenID provider metadata");
        }
        return data;
      } catch (Exception e) {
        if (failures == null) {
          failures = new ArrayList<>(WELL_KNOWN_PATHS.length);
        }
        failures.add(e);
      }
    }
    HttpClientException e =
        new HttpClientException("Failed to fetch OpenID provider metadata", failures.get(0));
    for (int i = 1; i < failures.size(); i++) {
      e.addSuppressed(failures.get(i));
    }
    throw e;
  }

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
