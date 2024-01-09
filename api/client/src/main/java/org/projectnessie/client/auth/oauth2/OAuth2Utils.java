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
import java.util.Random;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.Status;

class OAuth2Utils {

  private static final Random RANDOM = new SecureRandom();

  static String randomAlphaNumString(int length) {
    return RANDOM
        .ints('0', 'z' + 1)
        .filter(i -> (i <= '9') || (i >= 'A' && i <= 'Z') || (i >= 'a'))
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  public static JsonNode fetchOpenIdProviderMetadata(HttpClient httpClient, URI issuerUrl) {
    HttpResponse response =
        httpClient.newRequest(issuerUrl).path(".well-known/openid-configuration").get();
    Status status = response.getStatus();
    if (status != Status.OK) {
      throw new HttpClientException(
          "OpenID provider metadata request returned status code " + status.getCode());
    }
    JsonNode data = response.readEntity(JsonNode.class);
    if (!data.has("issuer") || !data.has("authorization_endpoint")) {
      throw new HttpClientException("Invalid OpenID provider metadata");
    }
    return data;
  }
}
