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

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class TestSecret {

  @Test
  void length() {
    Secret secret = new Secret("secret".toCharArray());
    assertThat(secret.length()).isEqualTo(6);
  }

  @Test
  void getCharsAndClear() {
    Secret secret = new Secret("secret");
    char[] chars = secret.getCharsAndClear();
    assertThat(chars).hasSize(6).containsExactly('s', 'e', 'c', 'r', 'e', 't');
    assertThat(secret.value).containsOnly('\0');
  }

  @Test
  void getStringAndClear() {
    Secret secret = new Secret("secret");
    String string = secret.getStringAndClear();
    assertThat(string).isEqualTo("secret");
    assertThat(secret.value).containsOnly('\0');
  }

  @Test
  void getBytesAndClear() {
    Secret secret = new Secret("sécrèt");
    byte[] bytes = secret.getBytesAndClear(StandardCharsets.UTF_8);
    assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("sécrèt");
    assertThat(secret.value).containsOnly('\0');
  }
}
