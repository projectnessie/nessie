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

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

/**
 * Common base for {@link ClientCredentialsTokensRequest}, {@link PasswordTokensRequest}, {@link
 * RefreshTokensRequest} and {@link TokensExchangeRequest}.
 */
interface TokensRequestBase {

  /** REQUIRED. The authorization grant type. */
  @JsonProperty("grant_type")
  GrantType getGrantType();

  /**
   * OPTIONAL, if identical to the scope requested by the client; otherwise, REQUIRED. The scope of
   * the access token as described by <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-3.3">Section 3.3</a>.
   *
   * <p>In case of refresh, the requested scope MUST NOT include any scope not originally granted by
   * the resource owner, and if omitted is treated as equal to the scope originally granted by the
   * resource owner.
   */
  @Nullable
  @JsonProperty("scope")
  String getScope();
}
