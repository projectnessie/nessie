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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;

/**
 * A device authorization tokens request as defined in <a
 * href="https://tools.ietf.org/html/rfc8628#section-3.4">RFC 8628 Section 3.4</a>.
 *
 * <p>Example of request:
 *
 * <pre>
 *   POST /token HTTP/1.1
 *   Host: server.example.com
 *   Content-Type: application/x-www-form-urlencoded
 *
 *   grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code
 *       &device_code=GmRhmhcxhwAzkoEqiMEg_DnyEysNkuNhszIySk9eS
 *       &client_id=1406020730
 * </pre>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableDeviceCodeTokenRequest.class)
@JsonDeserialize(as = ImmutableDeviceCodeTokenRequest.class)
@JsonTypeName(GrantType.Constants.DEVICE_CODE)
interface DeviceCodeTokenRequest extends TokenRequestBase, PublicClientRequest {

  /** REQUIRED. Value MUST be set to "urn:ietf:params:oauth:grant-type:device_code" */
  @Value.Derived
  @Override
  default GrantType getGrantType() {
    return GrantType.DEVICE_CODE;
  }

  /**
   * REQUIRED. The device verification code, "device_code" from the device authorization response,
   * defined in Section 3.2.
   */
  @JsonProperty("device_code")
  String getDeviceCode();

  static Builder builder() {
    return ImmutableDeviceCodeTokenRequest.builder();
  }

  interface Builder
      extends TokenRequestBase.Builder<DeviceCodeTokenRequest>,
          PublicClientRequest.Builder<DeviceCodeTokenRequest> {

    @CanIgnoreReturnValue
    Builder deviceCode(String deviceCode);
  }
}
