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
import org.projectnessie.tools.compatibility.api.Version;

class TestServerKey {
  @Test
  void nulls() {
    assertThatThrownBy(() -> new ServerKey(null, "abc", emptyMap()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ServerKey(Version.CURRENT, null, emptyMap()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ServerKey(Version.CURRENT, "abc", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void equalsHash() {
    Supplier<ServerKey> factory =
        () -> new ServerKey(Version.CURRENT, "foo", singletonMap("key", "value"));

    assertThat(factory.get())
        .isEqualTo(factory.get())
        .isNotEqualTo(new ServerKey(Version.CURRENT, "foo", emptyMap()))
        .isNotEqualTo(new ServerKey(Version.CURRENT, "bar", singletonMap("key", "value")))
        .isNotEqualTo(new ServerKey(Version.NOT_CURRENT, "foo", singletonMap("key", "value")))
        .isNotEqualTo("meep");
  }

  @Test
  void properties() {
    assertThat(
            new ServerKey(
                Version.CURRENT, "foo", ImmutableSortedMap.of("key", "value", "foo", "bar")))
        .extracting(
            ServerKey::getVersion,
            ServerKey::getDatabaseAdapterName,
            ServerKey::getDatabaseAdapterConfig,
            ServerKey::toString)
        .containsExactly(
            Version.CURRENT,
            "foo",
            ImmutableSortedMap.of("key", "value", "foo", "bar"),
            "server-current-foo-foo=bar_key=value");
  }
}
