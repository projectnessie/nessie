/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.internal;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSortedMap;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.tools.compatibility.api.Version;

class TestClientKey {
  @Test
  void nulls() {
    assertThatThrownBy(() -> new ClientKey(null, "abc", NessieApiV1.class, emptyMap()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ClientKey(Version.CURRENT, null, NessieApiV1.class, emptyMap()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ClientKey(Version.CURRENT, "abc", null, emptyMap()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ClientKey(Version.CURRENT, "abc", NessieApiV1.class, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equalsHash() {
    Supplier<ClientKey> factory =
        () ->
            new ClientKey(Version.CURRENT, "foo", NessieApiV1.class, singletonMap("key", "value"));

    assertThat(factory.get())
        .isEqualTo(factory.get())
        .isNotEqualTo(new ClientKey(Version.CURRENT, "foo", NessieApiV1.class, emptyMap()))
        .isNotEqualTo(
            new ClientKey(Version.CURRENT, "foo", NessieApi.class, singletonMap("key", "value")))
        .isNotEqualTo(
            new ClientKey(Version.CURRENT, "bar", NessieApiV1.class, singletonMap("key", "value")))
        .isNotEqualTo(
            new ClientKey(
                Version.NOT_CURRENT, "foo", NessieApiV1.class, singletonMap("key", "value")))
        .isNotEqualTo("meep");
  }

  @Test
  void properties() {
    assertThat(
            new ClientKey(
                Version.CURRENT,
                "foo",
                NessieApiV1.class,
                ImmutableSortedMap.of("key", "value", "foo", "bar")))
        .extracting(
            ClientKey::getVersion,
            ClientKey::getBuilderClass,
            ClientKey::getType,
            ClientKey::getConfigs,
            ClientKey::toString)
        .containsExactly(
            Version.CURRENT,
            "foo",
            NessieApiV1.class,
            ImmutableSortedMap.of("key", "value", "foo", "bar"),
            "client-current-org.projectnessie.client.api.NessieApiV1-foo-foo=bar_key=value");
  }
}
