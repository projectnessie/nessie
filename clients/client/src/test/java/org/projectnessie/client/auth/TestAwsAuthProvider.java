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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieConfigConstants;

class TestAwsAuthProvider {
  @Test
  void testAws() {
    Map<String, String> authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
            AwsAuthenticationProvider.AUTH_TYPE_VALUE,
            NessieConfigConstants.CONF_NESSIE_AWS_REGION,
            "eu-central-1");
    assertThat(NessieAuthenticationProvider.fromConfig(authCfg::get))
        .isNotNull()
        .extracting(Object::getClass)
        .extracting(Class::getSimpleName)
        .isEqualTo("AwsAuthentication");

    authCfg =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE, AwsAuthenticationProvider.AUTH_TYPE_VALUE);
    assertThat(NessieAuthenticationProvider.fromConfig(authCfg::get))
        .isNotNull()
        .extracting(Object::getClass)
        .extracting(Class::getSimpleName)
        .isEqualTo("AwsAuthentication");

    ImmutableMap<String, String> authCfgErr =
        ImmutableMap.of(
            NessieConfigConstants.CONF_NESSIE_AUTH_TYPE,
            AwsAuthenticationProvider.AUTH_TYPE_VALUE,
            NessieConfigConstants.CONF_NESSIE_AWS_REGION,
            "not-on-this-planet-1");
    assertThatThrownBy(() -> NessieAuthenticationProvider.fromConfig(authCfgErr::get))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown region 'not-on-this-planet-1'.");
  }
}
