/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.client.auth;

import java.net.HttpURLConnection;
import java.util.Map;

import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.http.HttpClient.Method;
import com.dremio.nessie.client.http.RequestFilter;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Filter to add auth header to all outgoing requests.
 */
public class AuthFilter implements RequestFilter {

  private final Auth auth;

  /**
   * construct auth filter depending on auth type.
   */
  public AuthFilter(AuthType authType, String username, String password) {
    switch (authType) {
      case AWS:
      case NONE:
        auth = new NoAuth();
        break;
      case BASIC:
        auth = new NoAuth(); // todo
        break;
      default:
        throw new IllegalStateException(String.format("%s does not exist", authType));
    }
  }

  private String checkKey() {
    return auth.checkKey();
  }

  @Override
  public void filter(HttpURLConnection con, String url, Map<String, String> headers,
                     Method method, Object body) {
    String header = checkKey();
    if (header != null && !header.isEmpty()) {
      headers.put("Authorization", header);
    }
  }

  @Override
  public void init(ObjectMapper mapper) {

  }
}
