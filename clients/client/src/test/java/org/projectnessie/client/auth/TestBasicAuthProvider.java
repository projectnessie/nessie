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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_USERNAME;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.RequestFilter;

class TestBasicAuthProvider {
  @Test
  void testNullParams() {
    assertAll(
        () ->
            assertThrows(
                NullPointerException.class,
                () ->
                    new BasicAuthenticationProvider()
                        .build(ImmutableMap.of(CONF_NESSIE_PASSWORD, "pass")::get)),
        () ->
            assertThrows(
                NullPointerException.class,
                () ->
                    new BasicAuthenticationProvider()
                        .build(ImmutableMap.of(CONF_NESSIE_USERNAME, "user")::get)),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> new BasicAuthenticationProvider().build(s -> null)));
  }

  @Test
  void testAuthEncoding() {
    Map<String, String> authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
            BasicAuthenticationProvider.AUTH_TYPE_VALUE,
            CONF_NESSIE_USERNAME,
            "Aladdin",
            CONF_NESSIE_PASSWORD,
            "OpenSesame");

    NessieAuthentication authentication = NessieAuthenticationProvider.fromConfig(authCfg::get);

    assertThat(authentication).isInstanceOf(HttpAuthentication.class);
    HttpAuthentication httpAuthentication = (HttpAuthentication) authentication;

    RequestFilter authFilter = httpAuthentication.buildRequestFilter();
    Map<String, Set<String>> map = new HashMap<>();
    RequestContext context = new RequestContext(map, null, null, null);
    authFilter.filter(context);

    assertEquals("Basic QWxhZGRpbjpPcGVuU2VzYW1l", map.get("Authorization").iterator().next());
  }
}
