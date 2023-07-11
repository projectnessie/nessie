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
package org.projectnessie.catalog.server.auth;

import javax.enterprise.inject.Vetoed;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.projectnessie.catalog.api.errors.OAuthTokenEndpointException;
import org.projectnessie.catalog.api.model.OAuthTokenRequest;
import org.projectnessie.catalog.service.spi.OAuthHandler;

@Vetoed
public class OAuthDisabledHandler implements OAuthHandler {

  @Override
  public OAuthTokenResponse getToken(OAuthTokenRequest request) {
    throw new OAuthTokenEndpointException(
        503, // service unavailable
        "OAuthTokenEndpointUnavailable",
        "OAuth token endpoint is unavailable");
  }
}
