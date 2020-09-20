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

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;

import com.dremio.nessie.client.NessieClient.AuthType;

/**
 * Filter to add auth header to all outgoing requests.
 */
public class AuthFilter implements ClientRequestFilter {

  private final Auth auth;

  /**
   * construct auth filter depending on auth type.
   */
  public AuthFilter(AuthType authType, String username, String password, WebTarget target) {
    switch (authType) {
      case AWS:
      case NONE:
        auth = new NoAuth();
        break;
      case BASIC:
        auth = new NoAuth();//todo
        break;
      default:
        throw new IllegalStateException(String.format("%s does not exist", authType));
    }
  }

  private String checkKey() {
    return auth.checkKey();
  }

  @Override
  public void filter(ClientRequestContext requestContext) {
    String header = checkKey();
    if (header != null && !header.isEmpty()) {
      requestContext.getHeaders().putSingle(HttpHeaders.AUTHORIZATION, header);
    }
  }
}
