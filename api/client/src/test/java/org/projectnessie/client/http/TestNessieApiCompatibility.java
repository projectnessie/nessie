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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.net.URI;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.NessieConfiguration;

@ExtendWith(MockitoExtension.class)
@WireMockTest
class TestNessieApiCompatibility {

  enum Expectation {
    OK,
    TOO_OLD,
    TOO_NEW,
    MISMATCH
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "1, 1, 1, 1, OK",
        "1, 1, 2, 1, OK",
        "1, 1, 2, 2, MISMATCH", // v2 endpoint mistakenly called with v1 client
        "1, 2, 2, 0, TOO_OLD",
        "2, 1, 1, 0, TOO_NEW",
        "2, 1, 2, 1, MISMATCH", // v1 endpoint mistakenly called with v2 client
        "2, 1, 2, 2, OK",
        "2, 2, 2, 2, OK",
      })
  void checkApiCompatibility(
      int client,
      int serverMin,
      int serverMax,
      int serverActual,
      Expectation expectation,
      WireMockRuntimeInfo wireMock) {

    NessieConfiguration config =
        ImmutableNessieConfiguration.builder()
            .minSupportedApiVersion(serverMin)
            .maxSupportedApiVersion(serverMax)
            .defaultBranch("main")
            .build();

    stubFor(get("/config").willReturn(ResponseDefinitionBuilder.okForJson(config)));
    stubFor(get("/trees/tree/main").willReturn(serverActual == 1 ? ok() : notFound()));

    try (HttpClient httpClient =
        HttpClient.builder()
            .setBaseUri(URI.create(wireMock.getHttpBaseUrl()))
            .setObjectMapper(new ObjectMapper())
            .addResponseFilter(new NessieHttpResponseFilter())
            .build()) {

      if (expectation == Expectation.OK) {

        assertThatCode(() -> NessieApiCompatibility.check(client, httpClient))
            .doesNotThrowAnyException();

      } else {

        assertThatThrownBy(() -> NessieApiCompatibility.check(client, httpClient))
            .hasMessageContaining(
                expectation == Expectation.MISMATCH
                    ? "mismatch"
                    : expectation == Expectation.TOO_OLD ? "too old" : "too new")
            .asInstanceOf(type(NessieApiCompatibilityException.class))
            .extracting(
                NessieApiCompatibilityException::getClientApiVersion,
                NessieApiCompatibilityException::getMinServerApiVersion,
                NessieApiCompatibilityException::getMaxServerApiVersion,
                NessieApiCompatibilityException::getActualServerApiVersion)
            .containsExactly(client, serverMin, serverMax, serverActual);
      }
    }
  }
}
