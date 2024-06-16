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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;

/**
 * Common interface for requests using grant types compatible with public clients.
 *
 * @see AuthorizationCodeTokenRequest
 * @see DeviceCodeRequest
 * @see PasswordTokenRequest
 * @see RefreshTokenRequest
 * @see TokenExchangeRequest
 */
interface PublicClientRequest {

  /**
   * The client identifier as described in Section 2.2 of [RFC6749]. REQUIRED if the client is not
   * authenticating with the authorization server as described in Section 3.2.1. of [RFC6749].
   */
  @JsonProperty("client_id")
  @Nullable
  String getClientId();

  interface Builder<T> {
    @CanIgnoreReturnValue
    Builder<T> clientId(String clientId);
  }
}
