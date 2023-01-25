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
package org.projectnessie.restcatalog.service.resources;

import static org.projectnessie.restcatalog.service.auth.OAuthRequest.oauthRequest;

import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.projectnessie.restcatalog.api.IcebergV1Oauth;
import org.projectnessie.restcatalog.service.auth.OAuthResponse;

public class IcebergV1OauthResource extends BaseIcebergResource implements IcebergV1Oauth {

  @Override
  public OAuthTokenResponse getToken(
      String grantType,
      String scope,
      String clientId,
      String clientSecret,
      TokenType requestedTokenType,
      String subjectToken,
      TokenType subjectTokenType,
      String actorToken,
      TokenType actorTokenType) {
    /*
    400:
      $ref: '#/components/responses/OAuthErrorResponse'
    401:
      $ref: '#/components/responses/OAuthErrorResponse'
    5XX:
      $ref: '#/components/responses/OAuthErrorResponse'
    */

    // TODO throw unauthorizedException("NotAuthorizedException", "not you do this");
    OAuthResponse oauthResponse =
        tenantSpecific
            .oauthHandler()
            .getToken(
                oauthRequest(
                    grantType,
                    scope,
                    clientId,
                    clientSecret,
                    requestedTokenType,
                    subjectToken,
                    subjectTokenType,
                    actorToken,
                    actorTokenType));

    OAuthTokenResponse.Builder responseBuilder =
        OAuthTokenResponse.builder()
            .addScope(oauthResponse.scope())
            .withToken(oauthResponse.accessToken())
            .withTokenType(oauthResponse.tokenType())
            .withIssuedTokenType(oauthResponse.issuedTokenType());
    Integer expiresIn = oauthResponse.expiresIn();
    if (expiresIn != null) {
      responseBuilder.setExpirationInSeconds(expiresIn);
    }

    return responseBuilder.build();
  }
}
