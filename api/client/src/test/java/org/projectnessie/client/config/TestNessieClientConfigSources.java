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
package org.projectnessie.client.config;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTH_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.client.config.NessieClientConfigSources.dotEnvFile;
import static org.projectnessie.client.config.NessieClientConfigSources.environmentConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.environmentFileConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.mapConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.nessieClientConfigFile;
import static org.projectnessie.client.config.NessieClientConfigSources.propertiesConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.propertiesFileConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.systemEnvironmentConfigSource;
import static org.projectnessie.client.config.NessieClientConfigSources.systemPropertiesConfigSource;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.config.NessieClientConfigSources.Credential;

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
    Files.write(f3, asList("A_B=foo", "B_C=bar", "C_D_MINUS=baz", "D_E=0"));

    soft.assertThat(asMap(environmentFileConfigSource(f1), "a.b", "b.c"))
        .containsEntry("a.b", null)
        .containsEntry("b.c", null);
    soft.assertThat(asMap(environmentFileConfigSource(f2), "a.b", "b.c"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", null);
    soft.assertThat(asMap(environmentFileConfigSource(f3), "a.b", "b.c", "c.d-minus", "d.e"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", "bar")
        .containsEntry("c.d-minus", "baz")
        .containsEntry("d.e", "0");
  }

  @Test
  void propertiesFileSource(@TempDir Path dir) throws IOException {
    Path f1 = dir.resolve("c_1");
    Path f2 = dir.resolve("c_2");
    Path f3 = dir.resolve("c_3");
    Files.createFile(f1);
    Files.write(f2, singletonList("a.b=foo"));
    Files.write(f3, asList("a.b=foo", "b.c=bar", "c.d-minus=baz", "d.e=0"));

    soft.assertThat(asMap(propertiesFileConfigSource(f1), "a.b", "b.c"))
        .containsEntry("a.b", null)
        .containsEntry("b.c", null);
    soft.assertThat(asMap(propertiesFileConfigSource(f2), "a.b", "b.c"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", null);
    soft.assertThat(asMap(propertiesFileConfigSource(f3), "a.b", "b.c", "c.d-minus", "d.e"))
        .containsEntry("a.b", "foo")
        .containsEntry("b.c", "bar")
        .containsEntry("c.d-minus", "baz")
        .containsEntry("d.e", "0");
  }

  @Test
  void systemEnvironmentSource() {
    NessieClientConfigSource configSource = systemEnvironmentConfigSource();

    Map<String, String> env = System.getenv();
    for (Map.Entry<String, String> e : env.entrySet()) {
      if (e.getKey().chars().anyMatch(c -> !(Character.isUpperCase(c) || c == '_'))) {
        // ignore env-vars with lower-case chars and chars not a '-'
        continue;
      }

      String k = e.getKey().replace('_', '.').toLowerCase(Locale.ROOT);
      soft.assertThat(configSource.getValue(k))
          .describedAs("Env var %s, using %s", e.getKey(), k)
          .isEqualTo(e.getValue());
    }
  }

  @Test
  void systemPropertiesSource() {
    NessieClientConfigSource configSource = systemPropertiesConfigSource();

    for (Map.Entry<Object, Object> e : System.getProperties().entrySet()) {
      soft.assertThat(configSource.getValue(e.getKey().toString()))
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

  @ParameterizedTest
  @MethodSource
  void fromIcebergRestCatalogProperties(
      Map<String, String> properties, Map<String, String> expected) {
    NessieClientConfigSource configSource =
        NessieClientConfigSources.fromIcebergRestCatalogProperties(properties);
    Map<String, String> produced =
        expected.keySet().stream().collect(Collectors.toMap(identity(), configSource::getValue));
    soft.assertThat(produced).containsExactlyInAnyOrderEntriesOf(expected);
  }

  static Stream<Arguments> fromIcebergRestCatalogProperties() {
    return Stream.of(
        arguments(
            ImmutableMap.of(
                "nessie.is-nessie-catalog", "true",
                "nessie.core-base-uri", "http://localhost/api/",
                "oauth2-server-uri", "http://oauth-stuff.internal/",
                "credential", "id:secret",
                "more", "more-value",
                "other", "other-value"),
            ImmutableMap.of(
                CONF_NESSIE_URI,
                "http://localhost/api/v2",
                "nessie.client-api-version",
                "2",
                CONF_NESSIE_OAUTH2_AUTH_ENDPOINT,
                "http://oauth-stuff.internal/",
                CONF_NESSIE_OAUTH2_CLIENT_ID,
                "id",
                CONF_NESSIE_OAUTH2_CLIENT_SECRET,
                "secret",
                CONF_NESSIE_OAUTH2_CLIENT_SCOPES,
                "catalog",
                CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN,
                "PT1H",
                //
                "more",
                "more-value",
                "nessie.other",
                "other-value")));
  }

  @ParameterizedTest
  @MethodSource
  void resolveTokenLifespan(Map<String, String> properties, String expected) {
    soft.assertThat(NessieClientConfigSources.resolveTokenLifespan(properties)).isEqualTo(expected);
  }

  static Stream<Arguments> resolveTokenLifespan() {
    return Stream.of(
        arguments(singletonMap("token-expires-in-ms", "1234"), "PT1.234S"),
        arguments(emptyMap(), "PT1H"));
  }

  @ParameterizedTest
  @MethodSource
  void resolveOAuthEndpoint(Map<String, String> properties, String expected) {
    soft.assertThat(properties).extractingByKey("oauth2-server-uri").isEqualTo(expected);
  }

  static Stream<Arguments> resolveOAuthEndpoint() {
    return Stream.of(
        arguments(singletonMap("oauth2-server-uri", "http://foo.bar/"), "http://foo.bar/"),
        arguments(singletonMap("nessie.iceberg-base-uri", "http://foo.bar/iceberg/"), null));
  }

  @ParameterizedTest
  @MethodSource
  void resolveOAuthScope(Map<String, String> properties, Set<String> expected) {
    String resolved = NessieClientConfigSources.resolveOAuthScope(properties);
    soft.assertThat(resolved).isNotNull();
    List<String> list = Arrays.stream(resolved.split(" ")).collect(Collectors.toList());
    soft.assertThat(list).containsExactlyInAnyOrderElementsOf(expected).hasSize(expected.size());
  }

  static Stream<Arguments> resolveOAuthScope() {
    return Stream.of(
        arguments(emptyMap(), singleton("catalog")),
        arguments(
            singletonMap(CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "myscope"),
            ImmutableSet.of("myscope", "catalog")),
        arguments(ImmutableMap.of("scope", "ibscope"), ImmutableSet.of("catalog", "ibscope")),
        arguments(
            ImmutableMap.of(
                CONF_NESSIE_OAUTH2_CLIENT_SCOPES,
                " myscope   otherscope ",
                "scope",
                "ibscope scope2"),
            ImmutableSet.of("myscope", "catalog", "ibscope", "otherscope", "scope2")),
        arguments(
            ImmutableMap.of(CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "myscope ", "scope", " ibscope"),
            ImmutableSet.of("myscope", "catalog", "ibscope")));
  }

  @ParameterizedTest
  @MethodSource
  void resolveCredential(Map<String, String> properties, Credential expected) {
    soft.assertThat(NessieClientConfigSources.resolveCredential(properties)).isEqualTo(expected);
  }

  static Stream<Arguments> resolveCredential() {
    return Stream.of(
        arguments(emptyMap(), new Credential(null, null)),
        arguments(singletonMap("credential", "myid:secret"), new Credential("myid", "secret")),
        arguments(
            ImmutableMap.of(CONF_NESSIE_OAUTH2_CLIENT_ID, "nessieid", "credential", "myid:secret"),
            new Credential("nessieid", "secret")),
        arguments(
            ImmutableMap.of(
                CONF_NESSIE_OAUTH2_CLIENT_SECRET, "nessiesecret", "credential", "myid:secret"),
            new Credential("myid", "nessiesecret")),
        arguments(
            ImmutableMap.of(
                CONF_NESSIE_OAUTH2_CLIENT_ID,
                "nessieid",
                CONF_NESSIE_OAUTH2_CLIENT_SECRET,
                "nessiesecret",
                "credential",
                "myid:secret"),
            new Credential("nessieid", "nessiesecret")));
  }

  @ParameterizedTest
  @MethodSource
  void resolveViaEnvironment(
      Map<String, String> properties, String property, String defaultValue, String expected) {
    soft.assertThat(
            NessieClientConfigSources.resolveViaEnvironment(properties, property, defaultValue))
        .isEqualTo(expected);
  }

  static Stream<Arguments> resolveViaEnvironment() {
    return Stream.of(
        arguments(emptyMap(), "something", null, null),
        arguments(emptyMap(), "something", "fallback", "fallback"),
        arguments(singletonMap("something", "value"), "something", null, "value"),
        arguments(singletonMap("something", "value"), "something", "fallback", "value"),
        arguments(singletonMap("something", "env:USER"), "something", null, System.getenv("USER")),
        arguments(
            singletonMap("something", "env:USER"), "something", "fallback", System.getenv("USER")));
  }

  @ParameterizedTest
  @MethodSource
  void parseIcebergCredential(String input, Credential expected) {
    soft.assertThat(NessieClientConfigSources.parseIcebergCredential(input)).isEqualTo(expected);
  }

  static Stream<Arguments> parseIcebergCredential() {
    return Stream.of(
        arguments(null, new Credential(null, null)),
        arguments("mysecret", new Credential(null, "mysecret")),
        arguments("myid:mysecret", new Credential("myid", "mysecret")));
  }

  static Map<String, String> asMap(NessieClientConfigSource configSource, String... keys) {
    Map<String, String> r = new HashMap<>();
    for (String key : keys) {
      r.put(key, configSource.getValue(key));
    }
    return r;
  }
}
