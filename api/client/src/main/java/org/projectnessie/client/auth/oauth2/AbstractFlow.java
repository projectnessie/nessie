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

  <REQ extends TokensRequestBase, RESP extends TokensResponseBase> RESP invokeTokenEndpoint(
      TokensRequestBase.Builder<REQ> request, Class<? extends RESP> responseClass) {
    request.scope(config.getScope().orElse(null));
    if (request instanceof PublicTokensRequestBase.Builder
        && !config.getClientSecret().isPresent()) {
      ((PublicTokensRequestBase.Builder<?>) request).clientId(config.getClientId());
    }
    return invokeEndpoint(config.getResolvedTokenEndpoint(), request.build(), responseClass);
  }

  DeviceCodeResponse invokeDeviceAuthEndpoint() {
    DeviceCodeRequest.Builder request =
        ImmutableDeviceCodeRequest.builder().scope(config.getScope().orElse(null));
    if (!config.getClientSecret().isPresent()) {
      request.clientId(config.getClientId());
    }
    return invokeEndpoint(
        config.getResolvedDeviceAuthEndpoint(), request.build(), DeviceCodeResponse.class);
  }

  private <REQ, RESP> RESP invokeEndpoint(
      URI endpoint, REQ request, Class<? extends RESP> responseClass) {
    HttpRequest req = config.getHttpClient().newRequest(endpoint);
    config.getBasicAuthentication().ifPresent(req::authentication);
    HttpResponse response = req.postForm(request);
    return response.readEntity(responseClass);
  }
}
