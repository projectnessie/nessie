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
import static org.projectnessie.tools.compatibility.api.Version.NEW_STORAGE_MODEL_WITH_COMPAT_TESTING;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.testkit.engine.EngineExecutionResults;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Event;
import org.junit.platform.testkit.engine.Events;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.junit.engine.MultiEnvTestEngine;
import org.projectnessie.junit.engine.MultiEnvTestFilter;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;

@ExtendWith(SoftAssertionsExtension.class)
class TestNessieCompatibilityExtensions {
  @InjectSoftAssertions protected SoftAssertions soft;

  private void assertNoFailedTestEvents(EngineExecutionResults result) {
    soft.assertThat(result.testEvents().count())
        .withFailMessage("Failed to run any tests")
        .isGreaterThan(0);

    Events failedEvents = result.testEvents().failed();
    soft.assertThat(failedEvents.count())
        .withFailMessage(
            () ->
                "The following test events have failed:\n:"
                    + failedEvents.stream().map(Event::toString).collect(Collectors.joining()))
        .isEqualTo(0);
  }

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
  @DisabledOnOs(
      value = OS.MAC,
      disabledReason = "Uses NessieUpgradesExtension, which is not compatible with macOS")
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
                      .configurationParameter(
                          "nessie.versions",
                          NEW_STORAGE_MODEL_WITH_COMPAT_TESTING.toString() + ",0.59.0")
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
    EngineExecutionResults results =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .configurationParameter(
                "nessie.versions", NEW_STORAGE_MODEL_WITH_COMPAT_TESTING.toString() + ",current")
            .selectors(selectClass(OldClientsSample.class))
            .execute();
    assertNoFailedTestEvents(results);

    soft.assertThat(OldClientsSample.allVersions)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING, Version.CURRENT);
    soft.assertThat(OldClientsSample.minVersionHigh).containsExactly(Version.CURRENT);
    soft.assertThat(OldClientsSample.maxVersionHigh)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING);
    soft.assertThat(OldClientsSample.never).isEmpty();
  }

  @Test
  void olderServers() {
    EngineExecutionResults results =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .configurationParameter(
                "nessie.versions", NEW_STORAGE_MODEL_WITH_COMPAT_TESTING.toString() + ",current")
            .selectors(selectClass(OldServersSample.class))
            .execute();
    assertNoFailedTestEvents(results);

    soft.assertThat(OldServersSample.allVersions)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING, Version.CURRENT);
    soft.assertThat(OldServersSample.minVersionHigh).containsExactly(Version.CURRENT);
    soft.assertThat(OldServersSample.maxVersionHigh)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING);
    soft.assertThat(OldServersSample.never).isEmpty();

    // Base URI should not include the API version suffix
    soft.assertThat(OldServersSample.uris).allMatch(uri -> uri.getPath().equals("/"));
  }

  @Test
  void injectedNessieApiUrlChanged() {
    final Version versionBeforeApiUrlChange = Version.parseVersion("0.74.0");
    EngineExecutionResults results =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .configurationParameter(
                "nessie.versions",
                versionBeforeApiUrlChange + "," + Version.NESSIE_URL_API_SUFFIX + ",current")
            .selectors(selectClass(ApiEndpointServerSample.class))
            .execute();
    assertNoFailedTestEvents(results);

    soft.assertThat(ApiEndpointServerSample.allVersions)
        .containsExactly(versionBeforeApiUrlChange, Version.NESSIE_URL_API_SUFFIX, Version.CURRENT);
  }

  @Test
  void nestedTests() {
    EngineExecutionResults results =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .configurationParameter(
                "nessie.versions", NEW_STORAGE_MODEL_WITH_COMPAT_TESTING.toString() + ",current")
            .selectors(selectClass(OuterSample.class))
            .selectors(selectClass(OuterSample.Inner.class))
            .filters(new MultiEnvTestFilter())
            .execute();
    assertNoFailedTestEvents(results);

    soft.assertThat(OuterSample.outerVersions)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING, Version.CURRENT);
    soft.assertThat(OuterSample.innerVersions).containsExactlyElementsOf(OuterSample.outerVersions);
  }

  @Test
  @DisabledOnOs(
      value = OS.MAC,
      disabledReason = "Uses NessieUpgradesExtension, which is not compatible with macOS")
  void upgrade() {
    EngineExecutionResults results =
        EngineTestKit.engine(MultiEnvTestEngine.ENGINE_ID)
            .configurationParameter(
                "nessie.versions", NEW_STORAGE_MODEL_WITH_COMPAT_TESTING.toString() + ",current")
            .selectors(selectClass(UpgradeSample.class))
            .execute();
    assertNoFailedTestEvents(results);

    soft.assertThat(UpgradeSample.allVersions)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING, Version.CURRENT);
    soft.assertThat(UpgradeSample.minVersionHigh).containsExactly(Version.CURRENT);
    soft.assertThat(UpgradeSample.maxVersionHigh)
        .containsExactly(NEW_STORAGE_MODEL_WITH_COMPAT_TESTING);
    soft.assertThat(UpgradeSample.never).isEmpty();
  }

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
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

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
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

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
  @ExtendWith({OlderNessieServersExtension.class, SoftAssertionsExtension.class})
  static class ApiEndpointServerSample {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessieAPI NessieApiV1 api;
    @NessieAPI static NessieApiV1 apiStatic;
    @NessieBaseUri URI uri;
    @NessieBaseUri static URI uriStatic;
    @NessieVersion Version version;
    @NessieVersion static Version versionStatic;

    static final List<Version> allVersions = new ArrayList<>();

    @Test
    void testSome() {
      soft.assertThat(api).isNotNull().isSameAs(apiStatic);
      soft.assertThat(uri).isNotNull().isEqualTo(uriStatic);
      soft.assertThat(version).isNotNull().isEqualTo(versionStatic);
      allVersions.add(version);

      // use api
      List<Reference> references = api.getAllReferences().get().getReferences();
      soft.assertThat(references).isNotEmpty();
    }
  }

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
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

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
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

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
  @ExtendWith(OlderNessieClientsExtension.class)
  @ExtendWith(OlderNessieServersExtension.class)
  static class TooManyExtensions1 {
    @Test
    void testSome() {}
  }

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
  @ExtendWith(OlderNessieClientsExtension.class)
  @ExtendWith(NessieUpgradesExtension.class)
  static class TooManyExtensions2 {
    @Test
    void testSome() {}
  }

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
  @ExtendWith(NessieUpgradesExtension.class)
  @ExtendWith(OlderNessieServersExtension.class)
  static class TooManyExtensions3 {
    @Test
    void testSome() {}
  }

  @SuppressWarnings({"JUnitMalformedDeclaration", "NewClassNamingConvention"})
  @ExtendWith(OlderNessieClientsExtension.class)
  @ExtendWith(OlderNessieServersExtension.class)
  @ExtendWith(NessieUpgradesExtension.class)
  static class TooManyExtensions4 {
    @Test
    void testSome() {}
  }
}
