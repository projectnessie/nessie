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
package org.projectnessie.restcatalog.server;

import jakarta.enterprise.context.ApplicationScoped;
import org.projectnessie.restcatalog.service.auth.OAuthHandler;
import org.projectnessie.restcatalog.service.auth.OAuthRequest;
import org.projectnessie.restcatalog.service.auth.OAuthResponse;

@ApplicationScoped
public class DelegatingOAuthHandler implements OAuthHandler {

  @Override
  public OAuthResponse getToken(OAuthRequest oauthRequest) {
    // TODO
    return OAuthResponse.oauthResponse("tok", "tok", "bearer", 42, "tok");
  }
}
