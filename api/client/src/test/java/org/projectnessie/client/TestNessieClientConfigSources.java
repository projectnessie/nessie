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
package org.projectnessie.client;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.projectnessie.client.NessieClientConfigSources.dotEnvFile;
import static org.projectnessie.client.NessieClientConfigSources.environmentConfigSource;
import static org.projectnessie.client.NessieClientConfigSources.environmentFileConfigSource;
import static org.projectnessie.client.NessieClientConfigSources.mapConfigSource;
import static org.projectnessie.client.NessieClientConfigSources.nessieClientConfigFile;
import static org.projectnessie.client.NessieClientConfigSources.propertiesConfigSource;
import static org.projectnessie.client.NessieClientConfigSources.propertiesFileConfigSource;
import static org.projectnessie.client.NessieClientConfigSources.systemEnvironmentConfigSource;
import static org.projectnessie.client.NessieClientConfigSources.systemPropertiesConfigSource;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieClientConfigSources {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void mapSource() {
    soft.assertThat(asMap(mapConfigSource(emptyMap()), "a", "b"))
        .containsEntry("a", null)
        .containsEntry("b", null);
    soft.assertThat(asMap(mapConfigSource(singletonMap("a", "foo")), "a", "b"))
        .containsEntry("a", "foo")
        .containsEntry("b", null);
    soft.assertThat(
            asMap(
                mapConfigSource(ImmutableMap.of("a", "foo", "b", "bar", "c", "baz")),
                "a",
                "b",
                "c"))
        .containsEntry("a", "foo")
        .containsEntry("b", "bar")
        .containsEntry("c", "baz");
  }

  @Test
  void propertiesSource() {
    soft.assertThat(asMap(propertiesConfigSource(new Properties()), "a", "b"))
        .containsEntry("a", null)
        .containsEntry("b", null);

    Properties p = new Properties();
    p.put("a", "foo");
    soft.assertThat(asMap(propertiesConfigSource(p), "a", "b"))
        .containsEntry("a", "foo")
        .containsEntry("b", null);

    p = new Properties();
    p.put("a", "foo");
    p.put("b", "bar");
    p.put("c", "baz");
    soft.assertThat(asMap(propertiesConfigSource(p), "a", "b", "c"))
        .containsEntry("a", "foo")
        .containsEntry("b", "bar")
        .containsEntry("c", "baz");
  }

  @Test
  void environmentSource() {
    soft.assertThat(asMap(environmentConfigSource(emptyMap()), "a.b", "b.c"))
        .containsEntry("a.b", null)
        .containsEntry("b.c", null);
    soft.assertThat(asMap(environmentConfigSource(singletonMap("A_B", "foo")), "a.b", "b.c"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", null);
    soft.assertThat(
            asMap(
                environmentConfigSource(
                    ImmutableMap.of("A_B", "foo", "B_C", "bar", "C_D_MINUS", "baz")),
                "a.b",
                "b.c",
                "c.d-minus"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", "bar")
        .containsEntry("c.d-minus", "baz");
  }

  @Test
  void environmentFileSource(@TempDir Path dir) throws IOException {
    Path f1 = dir.resolve("c_1");
    Path f2 = dir.resolve("c_2");
    Path f3 = dir.resolve("c_3");
    Files.createFile(f1);
    Files.write(f2, singletonList("A_B=foo"));
    Files.write(f3, asList("A_B=foo", "B_C=bar", "C_D_MINUS=baz"));

    soft.assertThat(asMap(environmentFileConfigSource(f1), "a.b", "b.c"))
        .containsEntry("a.b", null)
        .containsEntry("b.c", null);
    soft.assertThat(asMap(environmentFileConfigSource(f2), "a.b", "b.c"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", null);
    soft.assertThat(asMap(environmentFileConfigSource(f3), "a.b", "b.c", "c.d-minus"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", "bar")
        .containsEntry("c.d-minus", "baz");
  }

  @Test
  void propertiesFileSource(@TempDir Path dir) throws IOException {
    Path f1 = dir.resolve("c_1");
    Path f2 = dir.resolve("c_2");
    Path f3 = dir.resolve("c_3");
    Files.createFile(f1);
    Files.write(f2, singletonList("a.b=foo"));
    Files.write(f3, asList("a.b=foo", "b.c=bar", "c.d-minus=baz"));

    soft.assertThat(asMap(propertiesFileConfigSource(f1), "a.b", "b.c"))
        .containsEntry("a.b", null)
        .containsEntry("b.c", null);
    soft.assertThat(asMap(propertiesFileConfigSource(f2), "a.b", "b.c"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", null);
    soft.assertThat(asMap(propertiesFileConfigSource(f3), "a.b", "b.c", "c.d-minus"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", "bar")
        .containsEntry("c.d-minus", "baz");
  }

  @Test
  void systemEnvironmentSource() {
    NessieClientConfigSources.ConfigSource configSource = systemEnvironmentConfigSource();

    Map<String, String> env = System.getenv();
    for (Map.Entry<String, String> e : env.entrySet()) {
      if (e.getKey().chars().anyMatch(c -> !(Character.isUpperCase(c) || c == '_'))) {
        // ignore env-vars with lower-case chars and chars not a '-'
        continue;
      }

      String k = e.getKey().replace('_', '.').toLowerCase(Locale.ROOT);
      soft.assertThat(configSource.apply(k))
          .describedAs("Env var %s, using %s", e.getKey(), k)
          .isEqualTo(e.getValue());
    }
  }

  @Test
  void systemPropertiesSource() {
    NessieClientConfigSources.ConfigSource configSource = systemPropertiesConfigSource();

    for (Map.Entry<Object, Object> e : System.getProperties().entrySet()) {
      soft.assertThat(configSource.apply(e.getKey().toString()))
          .describedAs("System property %s", e.getKey())
          .isEqualTo(e.getValue());
    }
  }

  @Test
  void dotEnvFilePath() {
    soft.assertThat(dotEnvFile().toAbsolutePath())
        .isEqualTo(Paths.get(System.getProperty("user.dir"), ".env").toAbsolutePath());
  }

  @Test
  void clientConfigFilePath() {
    soft.assertThat(nessieClientConfigFile().toAbsolutePath())
        .isEqualTo(
            Paths.get(System.getProperty("user.dir"), ".config/nessie/nessie-client.properties")
                .toAbsolutePath());
  }

  static Map<String, String> asMap(Function<String, String> configSource, String... keys) {
    Map<String, String> r = new HashMap<>();
    for (String key : keys) {
      r.put(key, configSource.apply(key));
    }
    return r;
  }
}
