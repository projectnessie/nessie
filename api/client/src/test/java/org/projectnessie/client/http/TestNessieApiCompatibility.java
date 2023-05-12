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
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.NessieConfiguration;

@ExtendWith(MockitoExtension.class)
class TestNessieApiCompatibility {

  @Mock NessieApiV1 apiV1;
  @Mock NessieApiV2 apiV2;

  @ParameterizedTest
  @CsvSource(
      value = {
        "1, 1, 1,      ",
        "1, 1, 2,      ",
        "1, 1, 2, 2.0.0", // mimic v2 endpoint mistakenly called with v1 client
        "1, 2, 2, 2.0.0",
        "2, 1, 1,      ",
        "2, 1, 2, 2.0.0",
        "2, 1, 2,      ", // mimic v1 endpoint mistakenly called with v2 client
        "2, 2, 2, 2.0.0",
        "2, 2, 3, 3.0.0",
        "2, 3, 3, 3.0.0"
      })
  void checkApiCompatibility(int client, int serverMin, int serverMax, String serverSpec) {
    NessieApiV1 input = client == 1 ? apiV1 : apiV2;
    NessieConfiguration config =
        ImmutableNessieConfiguration.builder()
            .minSupportedApiVersion(serverMin)
            .maxSupportedApiVersion(serverMax)
            .specVersion(serverSpec)
            .build();
    given(input.getConfig()).willReturn(config);
    boolean incompatible = client < serverMin || client > serverMax;
    boolean mismatch = client == 1 ^ serverSpec == null;
    boolean ok = !incompatible && !mismatch;
    if (ok) {
      assertThatCode(() -> NessieApiCompatibility.checkApiCompatibility(input))
          .doesNotThrowAnyException();
    } else if (incompatible) {
      assertThatThrownBy(() -> NessieApiCompatibility.checkApiCompatibility(input))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "API version "
                  + client
                  + " is not supported by the server. "
                  + "The server supports API versions from "
                  + serverMin
                  + " to "
                  + serverMax
                  + " inclusive.");
    } else {
      assertThatThrownBy(() -> NessieApiCompatibility.checkApiCompatibility(input))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "Server supports API version "
                  + client
                  + " but replied with API version "
                  + (client == 1 ? 2 : 1)
                  + ". "
                  + "Is the client configured with the wrong URI prefix?");
    }
  }
}
