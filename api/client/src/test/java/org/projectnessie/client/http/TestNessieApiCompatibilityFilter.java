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

import static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder.responseDefinition;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.net.URI;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.client.rest.NessieHttpResponseFilter;

@ExtendWith(MockitoExtension.class)
@WireMockTest
class TestNessieApiCompatibilityFilter {

  enum Expectation {
    OK,
    TOO_OLD,
    TOO_NEW,
    MISMATCH;

    public String expectedErrorMessage() {
      switch (this) {
        case TOO_OLD:
          return "too old";
        case TOO_NEW:
          return "too new";
        case MISMATCH:
          return "mismatch";
        default:
          return null;
      }
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "1, 1, 1, 0, OK",
        "1, 1, 2, 1, OK",
        "1, 1, 2, 2, MISMATCH", // v2 endpoint mistakenly called with v1 client
        "1, 2, 2, 2, TOO_OLD",
        "2, 1, 1, 1, TOO_NEW",
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

    ObjectNode config = JsonNodeFactory.instance.objectNode();
    config.set("minSupportedApiVersion", JsonNodeFactory.instance.numberNode(serverMin));
    config.set("maxSupportedApiVersion", JsonNodeFactory.instance.numberNode(serverMax));
    if (serverActual > 0) {
      config.set("actualApiVersion", JsonNodeFactory.instance.numberNode(serverActual));
    }

    stubFor(
        get("/config")
            .willReturn(
                responseDefinition()
                    .withStatus(HTTP_OK)
                    .withBody(config.toString())
                    .withHeader("Content-Type", "application/json")));

    HttpClient.Builder builder =
        HttpClient.builder()
            .setBaseUri(URI.create(wireMock.getHttpBaseUrl()))
            .setObjectMapper(new ObjectMapper());

    NessieApiCompatibilityFilter filter = new NessieApiCompatibilityFilter(client);

    try (HttpClient httpClient = builder.build()) {
      filter.setHttpClient(httpClient);

      if (expectation == Expectation.OK) {

        assertThatCode(() -> filter.filter(null)).doesNotThrowAnyException();

      } else {

        assertThatThrownBy(() -> filter.filter(null))
            .hasMessageContaining(expectation.expectedErrorMessage())
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

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void testSkipApiCompatibilityCheck(int clientApiVersion, WireMockRuntimeInfo wireMock) {

    stubFor(get("/config").willReturn(responseDefinition().withStatus(HTTP_NOT_FOUND)));

    try (HttpClient httpClient =
        HttpClient.builder()
            .setBaseUri(URI.create(wireMock.getHttpBaseUrl()))
            .setObjectMapper(new ObjectMapper())
            .addResponseFilter(new NessieHttpResponseFilter())
            .build()) {

      assertThatCode(() -> NessieApiCompatibilityFilter.check(clientApiVersion, httpClient))
          .doesNotThrowAnyException();
    }
  }
}
