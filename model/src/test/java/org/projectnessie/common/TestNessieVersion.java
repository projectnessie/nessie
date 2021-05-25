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
package org.projectnessie.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

class TestNessieVersion {
  @Test
  void checkNessieVersion() throws Exception {
    URL url =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("org/projectnessie/common/nessie.properties");
    assertThat(url).isNotNull();

    Properties props = new Properties();
    try (InputStream in = url.openConnection().getInputStream()) {
      props.load(in);
    }

    assertThat(props).extractingByKey("nessie.version").isNotNull();
    assertThat(Version.parse(props.getProperty("nessie.version")))
        .isGreaterThan(Version.parse("0.6.1"));

    assertThat(props).extractingByKey("nessie.min-api-version").isNotNull();
    assertThat(Version.parse(props.getProperty("nessie.min-api-version")))
        .isGreaterThanOrEqualTo(Version.parse("0.6.0"));

    assertThat(props.getProperty("nessie.version"))
        .is(new Condition<>(NessieVersion::isApiCompatible, "api compatible"));
    assertThat("0.5.1").isNot(new Condition<>(NessieVersion::isApiCompatible, "api compatible"));
    assertThatThrownBy(() -> NessieVersion.isApiCompatible(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("null version argument");
  }
}
