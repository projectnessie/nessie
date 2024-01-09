/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.impl.HttpHeaders;
import org.projectnessie.client.http.impl.RequestContextImpl;

class TestBasicAuthProvider {
  @SuppressWarnings({"deprecation", "resource"})
  @Test
  void testNullParams() {
    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        new BasicAuthenticationProvider()
                            .build(
                                ImmutableMap.of(NessieConfigConstants.CONF_NESSIE_PASSWORD, "pass")
                                    ::get))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(
                    () ->
                        new BasicAuthenticationProvider()
                            .build(
                                ImmutableMap.of(NessieConfigConstants.CONF_NESSIE_USERNAME, "user")
                                    ::get))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> new BasicAuthenticationProvider().build(s -> null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> BasicAuthenticationProvider.create(null, "pass"))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> BasicAuthenticationProvider.create("user", null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> BasicAuthenticationProvider.create(null, null))
                .isInstanceOf(NullPointerException.class));
  }

  @Test
  void testFromConfig() {
    @SuppressWarnings("deprecation")
    Map<String, String> authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
            BasicAuthenticationProvider.AUTH_TYPE_VALUE,
            NessieConfigConstants.CONF_NESSIE_USERNAME,
            "Aladdin",
            NessieConfigConstants.CONF_NESSIE_PASSWORD,
            "OpenSesame");

    NessieAuthentication authentication = NessieAuthenticationProvider.fromConfig(authCfg::get);
    checkAuth(authentication);
  }

  @Test
  void testFromConfigLegacy() {
    Map<String, String> authCfg =
        ImmutableMap.of(
            "nessie.auth-type",
            BasicAuthenticationProvider.AUTH_TYPE_VALUE,
            "nessie.username",
            "Aladdin",
            "nessie.password",
            "OpenSesame");

    NessieAuthentication authentication = NessieAuthenticationProvider.fromConfig(authCfg::get);
    checkAuth(authentication);
  }

  @Test
  void testStaticBuilder() {
    checkAuth(BasicAuthenticationProvider.create("Aladdin", "OpenSesame"));
  }

  void checkAuth(NessieAuthentication authentication) {
    assertThat(authentication).isInstanceOf(HttpAuthentication.class);
    HttpAuthentication httpAuthentication = (HttpAuthentication) authentication;

    HttpHeaders headers = new HttpHeaders();
    RequestContext context = new RequestContextImpl(headers, null, null, null);
    httpAuthentication.filter(context);

    assertThat(headers.asMap())
        .containsKey("Authorization")
        .extracting("Authorization", InstanceOfAssertFactories.iterable(String.class))
        .containsExactly("Basic QWxhZGRpbjpPcGVuU2VzYW1l");
  }
}
