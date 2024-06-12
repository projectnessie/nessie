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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * A device access token response as defined in <a
 * href="https://tools.ietf.org/html/rfc8628#section-3.5">RFC 8628 Section 3.5</a>.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableDeviceCodeTokenResponse.class)
@JsonDeserialize(as = ImmutableDeviceCodeTokenResponse.class)
interface DeviceCodeTokenResponse extends TokenResponseBase {}
