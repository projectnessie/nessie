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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;

class TestNessieCompatibilityExtensions {
  @Test
  void noVersions() {
    assertThat(
            EngineTestKit.engine(MultiNessieVersionsTestEngine.ENGINE_ID)
                .selectors(selectClass(OldClientsSample.class))
                .execute()
                .testEvents()
                .list())
        .isEmpty();
  }

  @Test
  void tooManyExtensions() {
    assertThat(
            Stream.of(
                TooManyExtensions1.class,
                TooManyExtensions2.class,
                TooManyExtensions3.class,
                TooManyExtensions4.class))
        .allSatisfy(
            c ->
                assertThatThrownBy(
                        () ->
                            EngineTestKit.engine(MultiNessieVersionsTestEngine.ENGINE_ID)
                                .configurationParameter("nessie.versions", "0.18.0,0.19.0")
                                .selectors(selectClass(c))
                                .filters(new ExcludeJunitEnginesFilter())
                                .execute())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageEndingWith(
                        " contains more than one Nessie multi-version extension"));
  }

  @Test
  void olderClients() {
    EngineTestKit.engine(MultiNessieVersionsTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.18.0,current")
        .selectors(selectClass(OldClientsSample.class))
        .execute();
    assertThat(OldClientsSample.allVersions)
        .containsExactly(Version.parseVersion("0.18.0"), Version.CURRENT);
    assertThat(OldClientsSample.minVersionHigh).containsExactly(Version.CURRENT);
    assertThat(OldClientsSample.maxVersionHigh).containsExactly(Version.parseVersion("0.18.0"));
    assertThat(OldClientsSample.never).isEmpty();
  }

  @Test
  void olderServers() {
    EngineTestKit.engine(MultiNessieVersionsTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.18.0,current")
        .selectors(selectClass(OldServersSample.class))
        .execute();
    assertThat(OldServersSample.allVersions)
        .containsExactly(Version.parseVersion("0.18.0"), Version.CURRENT);
    assertThat(OldServersSample.minVersionHigh).containsExactly(Version.CURRENT);
    assertThat(OldServersSample.maxVersionHigh).containsExactly(Version.parseVersion("0.18.0"));
    assertThat(OldServersSample.never).isEmpty();
  }

  @Test
  void upgrade() {
    EngineTestKit.engine(MultiNessieVersionsTestEngine.ENGINE_ID)
        .configurationParameter("nessie.versions", "0.18.0,current")
        .selectors(selectClass(UpgradeSample.class))
        .execute();
    assertThat(UpgradeSample.allVersions)
        .containsExactly(Version.parseVersion("0.18.0"), Version.CURRENT);
    assertThat(UpgradeSample.minVersionHigh).containsExactly(Version.CURRENT);
    assertThat(UpgradeSample.maxVersionHigh).containsExactly(Version.parseVersion("0.18.0"));
    assertThat(UpgradeSample.never).isEmpty();
  }

  @ExtendWith(OlderNessieClientsExtension.class)
  static class OldClientsSample {
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
      assertThat(api).isNotNull().isSameAs(apiStatic);
      assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);

      assertThat(api.getConfig())
          .extracting(NessieConfiguration::getDefaultBranch)
          .isEqualTo("main");
      assertThat(api.getDefaultBranch()).extracting(Branch::getName).isEqualTo("main");
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
  static class OldServersSample {
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
      assertThat(api).isNotNull().isSameAs(apiStatic);
      assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);

      assertThat(api.getConfig())
          .extracting(NessieConfiguration::getDefaultBranch)
          .isEqualTo("main");
      assertThat(api.getDefaultBranch()).extracting(Branch::getName).isEqualTo("main");
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

  @ExtendWith(NessieUpgradesExtension.class)
  static class UpgradeSample {
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
      assertThat(api).isNotNull().isSameAs(apiStatic);
      assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);

      assertThat(api.getConfig())
          .extracting(NessieConfiguration::getDefaultBranch)
          .isEqualTo("main");
      assertThat(api.getDefaultBranch()).extracting(Branch::getName).isEqualTo("main");
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
