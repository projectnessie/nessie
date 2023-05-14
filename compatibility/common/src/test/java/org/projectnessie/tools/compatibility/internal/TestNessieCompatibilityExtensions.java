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

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.testkit.engine.EngineExecutionResults;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.junit.engine.MultiEnvTestEngine;
import org.projectnessie.junit.engine.MultiEnvTestFilter;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;

@ExtendWith(SoftAssertionsExtension.class)
class TestNessieCompatibilityExtensions {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void noVersions() {
    soft.assertThatThrownBy(
            () ->
                EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
                    .selectors(selectClass(OldClientsSample.class))
                    .execute())
        .hasMessageContaining("TestEngine with ID 'nessie-multi-env' failed to discover tests")
        .cause()
        .hasMessageContaining(
            "MultiEnvTestEngine was enabled, but test extensions did not discover any environment IDs")
        .hasMessageContaining("OlderNessieClientsExtension");
  }

  @Test
  void tooManyExtensions() {
    soft.assertThat(
            Stream.of(
                TooManyExtensions1.class,
                TooManyExtensions2.class,
                TooManyExtensions3.class,
                TooManyExtensions4.class))
        .allSatisfy(
            c -> {
              EngineExecutionResults result =
                  EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
                      .configurationParameter("nessie.versions", "0.42.0,0.43.0")
                      .selectors(selectClass(c))
                      .filters(new MultiEnvTestFilter())
                      .execute();
              soft.assertThat(
                      result.allEvents().executions().failed().stream()
                          .map(e -> e.getTerminationInfo().getExecutionResult().getThrowable())
                          .filter(Optional::isPresent)
                          .map(Optional::get))
                  .isNotEmpty()
                  .allSatisfy(
                      e ->
                          soft.assertThat(e)
                              .isInstanceOf(IllegalStateException.class)
                              .hasMessageEndingWith(
                                  " contains more than one Nessie multi-version extension"));
            });
  }

  @Test
  void olderClients() {
    EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.42.0,current")
        .selectors(selectClass(OldClientsSample.class))
        .execute();
    soft.assertThat(OldClientsSample.allVersions)
        .containsExactly(Version.parseVersion("0.42.0"), Version.CURRENT);
    soft.assertThat(OldClientsSample.minVersionHigh).containsExactly(Version.CURRENT);
    soft.assertThat(OldClientsSample.maxVersionHigh)
        .containsExactly(Version.parseVersion("0.42.0"));
    soft.assertThat(OldClientsSample.never).isEmpty();
  }

  @Test
  void olderServers() {
    EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.42.0,current")
        .selectors(selectClass(OldServersSample.class))
        .execute();
    soft.assertThat(OldServersSample.allVersions)
        .containsExactly(Version.parseVersion("0.42.0"), Version.CURRENT);
    soft.assertThat(OldServersSample.minVersionHigh).containsExactly(Version.CURRENT);
    soft.assertThat(OldServersSample.maxVersionHigh)
        .containsExactly(Version.parseVersion("0.42.0"));
    soft.assertThat(OldServersSample.never).isEmpty();

    // Base URI should not include the API version suffix
    soft.assertThat(OldServersSample.uris).allMatch(uri -> uri.getPath().equals("/"));
  }

  @Test
  void nestedTests() {
    EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.42.0,current")
        .selectors(selectClass(OuterSample.class))
        .selectors(selectClass(OuterSample.Inner.class))
        .filters(new MultiEnvTestFilter())
        .execute();
    soft.assertThat(OuterSample.outerVersions)
        .containsExactly(Version.parseVersion("0.42.0"), Version.CURRENT);
    soft.assertThat(OuterSample.innerVersions).containsExactlyElementsOf(OuterSample.outerVersions);
  }

  @Test
  void upgrade() {
    EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.42.0,current")
        .selectors(selectClass(UpgradeSample.class))
        .execute();
    soft.assertThat(UpgradeSample.allVersions)
        .containsExactly(Version.parseVersion("0.42.0"), Version.CURRENT);
    soft.assertThat(UpgradeSample.minVersionHigh).containsExactly(Version.CURRENT);
    soft.assertThat(UpgradeSample.maxVersionHigh).containsExactly(Version.parseVersion("0.42.0"));
    soft.assertThat(UpgradeSample.never).isEmpty();
  }

  @ExtendWith({OlderNessieClientsExtension.class, SoftAssertionsExtension.class})
  static class OldClientsSample {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessieAPI NessieApiV1 api;
    @NessieAPI static NessieApiV1 apiStatic;
    @NessieVersion Version version;
    @NessieVersion static Version versionStatic;

    static final List<Version> allVersions = new ArrayList<>();
    static final List<Version> minVersionHigh = new ArrayList<>();
    static final List<Version> maxVersionHigh = new ArrayList<>();
    static final List<Version> never = new ArrayList<>();

    @Test
    void testSome() throws Exception {
      soft.assertThat(api).isNotNull().isSameAs(apiStatic);
      soft.assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);

      soft.assertThat(api.getConfig())
          .extracting(NessieConfiguration::getDefaultBranch)
          .isEqualTo("main");
      soft.assertThat(api.getDefaultBranch()).extracting(Branch::getName).isEqualTo("main");
    }

    @VersionCondition(minVersion = "99999")
    @Test
    void minVersionHigh() {
      minVersionHigh.add(version);
    }

    @VersionCondition(maxVersion = "99999")
    @Test
    void maxVersionHigh() {
      maxVersionHigh.add(version);
    }

    @VersionCondition(minVersion = "0.0.1", maxVersion = "0.0.2")
    @Test
    void never() {
      never.add(version);
    }
  }

  @ExtendWith({OlderNessieServersExtension.class, SoftAssertionsExtension.class})
  static class OldServersSample {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessieAPI NessieApiV1 api;
    @NessieAPI static NessieApiV1 apiStatic;
    @NessieBaseUri URI uri;
    @NessieBaseUri static URI uriStatic;
    @NessieVersion Version version;
    @NessieVersion static Version versionStatic;

    static final List<Version> allVersions = new ArrayList<>();
    static final List<Version> minVersionHigh = new ArrayList<>();
    static final List<Version> maxVersionHigh = new ArrayList<>();
    static final List<Version> never = new ArrayList<>();
    static final List<URI> uris = new ArrayList<>();

    @Test
    void testSome() throws Exception {
      soft.assertThat(api).isNotNull().isSameAs(apiStatic);
      soft.assertThat(uri).isNotNull().isEqualTo(uriStatic);
      soft.assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);
      uris.add(uri);

      soft.assertThat(api.getConfig())
          .extracting(NessieConfiguration::getDefaultBranch)
          .isEqualTo("main");
      soft.assertThat(api.getDefaultBranch()).extracting(Branch::getName).isEqualTo("main");
    }

    @VersionCondition(minVersion = "99999")
    @Test
    void minVersionHigh() {
      minVersionHigh.add(version);
    }

    @VersionCondition(maxVersion = "99999")
    @Test
    void maxVersionHigh() {
      maxVersionHigh.add(version);
    }

    @VersionCondition(minVersion = "0.0.1", maxVersion = "0.0.2")
    @Test
    void never() {
      never.add(version);
    }
  }

  @ExtendWith(OlderNessieServersExtension.class)
  static class OuterSample {
    static final List<Version> outerVersions = new ArrayList<>();
    static final List<Version> innerVersions = new ArrayList<>();

    @NessieVersion Version outerVersion;

    @Test
    void outer() {
      outerVersions.add(outerVersion);
    }

    @Nested
    class Inner {
      @NessieVersion Version innerVersion;

      @Test
      void inner() {
        innerVersions.add(innerVersion);
      }
    }
  }

  @ExtendWith({NessieUpgradesExtension.class, SoftAssertionsExtension.class})
  static class UpgradeSample {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessieAPI NessieApiV1 api;
    @NessieAPI static NessieApiV1 apiStatic;
    @NessieVersion Version version;
    @NessieVersion static Version versionStatic;

    static final List<Version> allVersions = new ArrayList<>();
    static final List<Version> minVersionHigh = new ArrayList<>();
    static final List<Version> maxVersionHigh = new ArrayList<>();
    static final List<Version> never = new ArrayList<>();

    @Test
    void testSome() throws Exception {
      soft.assertThat(api).isNotNull().isSameAs(apiStatic);
      soft.assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);

      soft.assertThat(api.getConfig())
          .extracting(NessieConfiguration::getDefaultBranch)
          .isEqualTo("main");
      soft.assertThat(api.getDefaultBranch()).extracting(Branch::getName).isEqualTo("main");
    }

    @VersionCondition(minVersion = "99999")
    @Test
    void minVersionHigh() {
      minVersionHigh.add(version);
    }

    @VersionCondition(maxVersion = "99999")
    @Test
    void maxVersionHigh() {
      maxVersionHigh.add(version);
    }

    @VersionCondition(minVersion = "0.0.1", maxVersion = "0.0.2")
    @Test
    void never() {
      never.add(version);
    }
  }

  @ExtendWith(OlderNessieClientsExtension.class)
  @ExtendWith(OlderNessieServersExtension.class)
  static class TooManyExtensions1 {
    @Test
    void testSome() {}
  }

  @ExtendWith(OlderNessieClientsExtension.class)
  @ExtendWith(NessieUpgradesExtension.class)
  static class TooManyExtensions2 {
    @Test
    void testSome() {}
  }

  @ExtendWith(NessieUpgradesExtension.class)
  @ExtendWith(OlderNessieServersExtension.class)
  static class TooManyExtensions3 {
    @Test
    void testSome() {}
  }

  @ExtendWith(OlderNessieClientsExtension.class)
  @ExtendWith(OlderNessieServersExtension.class)
  @ExtendWith(NessieUpgradesExtension.class)
  static class TooManyExtensions4 {
    @Test
    void testSome() {}
  }
}
