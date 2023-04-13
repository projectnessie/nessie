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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.NessieAuthenticationProvider;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.RequestFilter;
import org.projectnessie.client.http.impl.HttpHeaders;
import org.projectnessie.client.http.impl.RequestContextImpl;
import org.projectnessie.client.util.HttpTestServer;

class TestOAuth2AuthenticationProvider {

  @Test
  @SuppressWarnings("resource")
  void testNullParams() {
    assertThatThrownBy(() -> new OAuth2AuthenticationProvider().build(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new OAuth2AuthenticationProvider().build(prop -> null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OAuth2AuthenticationProvider.create(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testFromConfig() throws Exception {

    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          TokensResponseBase tokenResponse =
              ImmutableClientCredentialsTokensResponse.builder()
                  .accessTokenPayload("cafebabe")
                  .tokenType("bearer")
                  .build();
          writeResponseBody(resp, tokenResponse, "application/json");
        };

    try (HttpTestServer server = new HttpTestServer(handler)) {
      Map<String, String> authCfg =
          ImmutableMap.of(
              NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
              OAuth2AuthenticationProvider.AUTH_TYPE_VALUE,
              CONF_NESSIE_OAUTH2_CLIENT_ID,
              "Alice",
              CONF_NESSIE_OAUTH2_CLIENT_SECRET,
              "s3cr3t",
              CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT,
              server.getUri().toString());
      NessieAuthentication authentication = NessieAuthenticationProvider.fromConfig(authCfg::get);
      assertThat(authentication).extracting("authenticator").isInstanceOf(OAuth2Client.class);
    }
  }

  @Test
  void testStaticBuilder() {
    OAuth2Authenticator authenticator = Mockito.mock(OAuth2Authenticator.class);
    when(authenticator.authenticate())
        .thenReturn(ImmutableAccessToken.builder().payload("cafebabe").tokenType("bearer").build());
    HttpAuthentication authentication = OAuth2AuthenticationProvider.create(authenticator);
    assertThat(authentication).isInstanceOf(HttpAuthentication.class);

    // Intercept the call to HttpClient.register(RequestFilter) and extract the RequestFilter for
    // our test
    RequestFilter[] authFilter = new RequestFilter[1];
    HttpClient.Builder client = Mockito.mock(HttpClient.Builder.class);
    Mockito.doAnswer(
            invocationOnMock -> {
              Object[] args = invocationOnMock.getArguments();
              if (args.length == 1 && args[0] instanceof RequestFilter) {
                authFilter[0] = (RequestFilter) args[0];
              }
              return null;
            })
        .when(client)
        .addRequestFilter(Mockito.any());
    authentication.applyToHttpClient(client);

    // Check that the registered RequestFilter works as expected (sets the right HTTP header)

    assertThat(authFilter[0]).isInstanceOf(RequestFilter.class);

    HttpHeaders headers = new HttpHeaders();
    RequestContext context = new RequestContextImpl(headers, null, null, null);
    authFilter[0].filter(context);

    assertThat(headers.asMap())
        .containsKey("Authorization")
        .extracting("Authorization", InstanceOfAssertFactories.iterable(String.class))
        .containsExactly("Bearer cafebabe");

    verify(authenticator).authenticate();
  }

  @ParameterizedTest
  @MethodSource
  public void testCapitalize(String input, String expected) {
    String actual = OAuth2AuthenticationProvider.capitalize(input);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testCapitalize() {
    return Stream.of(
        Arguments.of(null, ""),
        Arguments.of("", ""),
        Arguments.of("a", "A"),
        Arguments.of("abc", "Abc"),
        Arguments.of("ABC", "Abc"),
        Arguments.of("AbC", "Abc"));
  }
}
