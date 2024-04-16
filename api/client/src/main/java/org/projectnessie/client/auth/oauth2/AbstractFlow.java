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

import java.net.URI;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.HttpResponse;

abstract class AbstractFlow implements Flow {

  final OAuth2ClientConfig config;

  AbstractFlow(OAuth2ClientConfig config) {
    this.config = config;
  }

  <T> T invokeEndpoint(URI endpoint, Object request, Class<? extends T> responseClass) {
    HttpRequest req = config.getHttpClient().newRequest(endpoint);
    config.getBasicAuthentication().ifPresent(req::authentication);
    HttpResponse response = req.postForm(request);
    return response.readEntity(responseClass);
  }

  <T extends TokensResponseBase> T invokeTokenEndpoint(
      TokensRequestBase request, Class<? extends T> responseClass) {
    return invokeEndpoint(config.getResolvedTokenEndpoint(), request, responseClass);
  }
}
