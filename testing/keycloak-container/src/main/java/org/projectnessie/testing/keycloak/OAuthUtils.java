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
package org.projectnessie.testing.keycloak;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

public final class OAuthUtils {
  private OAuthUtils() {}

  public static String fetchToken(URI tokenEndpoint, String clientId, String clientSecret) {
    // curl http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token --user client1:s3cr3t
    // -d 'grant_type=client_credentials' -d 'scope=catalog' | jq -r .access_token

    try {
      HttpURLConnection getToken = (HttpURLConnection) tokenEndpoint.toURL().openConnection();
      getToken.setRequestMethod("POST");
      getToken.setDoOutput(true);
      getToken.setRequestProperty(
          "Authorization",
          "Basic "
              + Base64.getEncoder()
                  .encodeToString((clientId + ":" + clientSecret).getBytes(UTF_8)));
      getToken.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      try (OutputStream out = getToken.getOutputStream()) {
        out.write("grant_type=client_credentials&scope=catalog".getBytes(UTF_8));
      }
      JsonNode tokenResponse =
          new ObjectMapper().readValue(getToken.getInputStream(), JsonNode.class);
      return tokenResponse.get("access_token").textValue();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
