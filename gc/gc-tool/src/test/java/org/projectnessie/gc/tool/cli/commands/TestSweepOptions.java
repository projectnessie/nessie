/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.gc.tool.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.gc.tool.cli.commands.TestMarkOptions.parseCommand;

import java.util.UUID;
import java.util.function.Predicate;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.tool.cli.options.SweepOptions;
import org.projectnessie.model.ContentKey;
import org.projectnessie.storage.uri.StorageUri;
import picocli.CommandLine.ParameterException;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSweepOptions {
  @InjectSoftAssertions private SoftAssertions soft;

  private static Sweep sweep(String... extraArgs) {
    String[] args = new String[5 + extraArgs.length];
    args[0] = "sweep";
    args[1] = "--jdbc-url";
    args[2] = "jdbc:foo:";
    args[3] = "--live-set-id";
    args[4] = UUID.randomUUID().toString();
    System.arraycopy(extraArgs, 0, args, 5, extraArgs.length);
    return parseCommand(args);
  }

  @Test
  public void noStoragePrefixMeansNoRestriction() {
    SweepOptions options = sweep().sweepOptions;

    soft.assertThat(options.getStoragePrefixes()).isEmpty();
    soft.assertThat(options.createBaseLocationFilter()).isEmpty();
  }

  @Test
  public void singleStoragePrefix() {
    SweepOptions options = sweep("--storage-prefix", "s3://bucket/team_a/").sweepOptions;

    Predicate<StorageUri> filter = options.createBaseLocationFilter().orElseThrow();
    assertThat(filter)
        .accepts(StorageUri.of("s3://bucket/team_a/orders/"))
        .rejects(StorageUri.of("s3://bucket/team_b/events/"), StorageUri.of("s3://bucket/"));
  }

  @Test
  public void repeatedAndCommaSeparatedStoragePrefixes() {
    SweepOptions options =
        sweep(
                "--storage-prefix",
                "s3://bucket/team_a/",
                "--storage-prefix",
                "s3://bucket/team_c/logs,s3://other/team_d")
            .sweepOptions;

    soft.assertThat(options.getStoragePrefixes())
        .containsExactly("s3://bucket/team_a/", "s3://bucket/team_c/logs", "s3://other/team_d");

    Predicate<StorageUri> filter = options.createBaseLocationFilter().orElseThrow();
    soft.assertThat(filter)
        .accepts(
            StorageUri.of("s3://bucket/team_a/orders/"),
            StorageUri.of("s3://bucket/team_c/logs/2026/"),
            StorageUri.of("s3://other/team_d/x/"))
        .rejects(
            StorageUri.of("s3://bucket/team_b/events/"),
            StorageUri.of("s3://bucket/team_c/"),
            StorageUri.of("s3://other/team_e/"));
  }

  @Test
  public void relativeStoragePrefixIsRejected() {
    SweepOptions options = sweep("--storage-prefix", "team_a/orders").sweepOptions;

    assertThatThrownBy(options::createBaseLocationFilter)
        .isInstanceOf(ParameterException.class)
        .hasMessageContaining("--storage-prefix")
        .hasMessageContaining("team_a/orders")
        .hasMessageContaining("absolute URI");
  }

  @Test
  public void emptyStoragePrefixIsRejected() {
    SweepOptions options = sweep("--storage-prefix", "  ").sweepOptions;

    assertThatThrownBy(options::validate)
        .isInstanceOf(ParameterException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  public void validateAcceptsGoodAndMissingPrefixes() {
    sweep().sweepOptions.validate();
    sweep("--storage-prefix", "s3://bucket/team_a/").sweepOptions.validate();
  }

  // --- --target-namespace ------------------------------------------------------------------

  @Test
  public void noTargetNamespaceMeansNoRestriction() {
    SweepOptions options = sweep().sweepOptions;

    soft.assertThat(options.getTargetNamespaces()).isEmpty();
    soft.assertThat(options.createContentKeyFilter()).isEmpty();
  }

  @Test
  public void singleTargetNamespace() {
    SweepOptions options = sweep("--target-namespace", "team_a").sweepOptions;

    Predicate<ContentKey> filter = options.createContentKeyFilter().orElseThrow();
    assertThat(filter)
        .accepts(ContentKey.of("team_a", "orders"), ContentKey.of("team_a", "customers"))
        .rejects(
            ContentKey.of("team_b", "orders"),
            ContentKey.of("team_a", "sales", "orders"),
            ContentKey.of("team_ab", "orders"),
            ContentKey.of("orders"));
  }

  @Test
  public void targetNamespaceMustMatchTheWholeNamespace() {
    SweepOptions options = sweep("--target-namespace", "team").sweepOptions;

    assertThat(options.createContentKeyFilter().orElseThrow())
        .describedAs("a part of the namespace is not enough")
        .rejects(ContentKey.of("team_a", "orders"), ContentKey.of("my_team", "orders"))
        .accepts(ContentKey.of("team", "orders"));
  }

  @Test
  public void targetNamespaceIsDotSeparated() {
    SweepOptions options = sweep("--target-namespace", "team_a\\.sales").sweepOptions;

    assertThat(options.createContentKeyFilter().orElseThrow())
        .accepts(ContentKey.of("team_a", "sales", "orders"))
        .rejects(
            ContentKey.of("team_a", "orders"),
            ContentKey.of("team_a", "sales", "emea", "orders"),
            ContentKey.of("team_aXsales", "orders"));
  }

  @Test
  public void targetNamespaceWithNestedNamespaces() {
    SweepOptions options = sweep("--target-namespace", "team_a(\\..*)?").sweepOptions;

    assertThat(options.createContentKeyFilter().orElseThrow())
        .accepts(
            ContentKey.of("team_a", "orders"),
            ContentKey.of("team_a", "sales", "orders"),
            ContentKey.of("team_a", "sales", "emea", "orders"))
        .rejects(ContentKey.of("team_ab", "orders"), ContentKey.of("team_b", "orders"));
  }

  @Test
  public void repeatedAndCommaSeparatedTargetNamespaces() {
    SweepOptions options =
        sweep("--target-namespace", "team_a", "--target-namespace", "team_c\\.logs,team_d")
            .sweepOptions;

    soft.assertThat(options.getTargetNamespaces())
        .containsExactly("team_a", "team_c\\.logs", "team_d");
    soft.assertThat(options.createContentKeyFilter().orElseThrow())
        .accepts(
            ContentKey.of("team_a", "orders"),
            ContentKey.of("team_c", "logs", "access"),
            ContentKey.of("team_d", "events"))
        .rejects(ContentKey.of("team_b", "orders"), ContentKey.of("team_c", "orders"));
  }

  @Test
  public void invalidTargetNamespaceRegularExpressionIsRejected() {
    SweepOptions options = sweep("--target-namespace", "team_a(").sweepOptions;

    assertThatThrownBy(options::createContentKeyFilter)
        .isInstanceOf(ParameterException.class)
        .hasMessageContaining("--target-namespace")
        .hasMessageContaining("team_a(");
  }

  @Test
  public void emptyTargetNamespaceIsRejected() {
    SweepOptions options = sweep("--target-namespace", "  ").sweepOptions;

    assertThatThrownBy(options::validate)
        .isInstanceOf(ParameterException.class)
        .hasMessageContaining("must not be empty");
  }

  @Test
  public void validateRejectsInvalidTargetNamespaceBeforeAnyWorkStarts() {
    SweepOptions gcOptions =
        ((MarkAndSweep)
                parseCommand("gc", "--jdbc-url", "jdbc:foo:", "--target-namespace", "(unclosed"))
            .sweepOptions;

    assertThatThrownBy(gcOptions::validate).isInstanceOf(ParameterException.class);
  }

  @Test
  public void targetNamespaceAndStoragePrefixCanBeCombined() {
    SweepOptions options =
        sweep("--target-namespace", "team_b", "--storage-prefix", "s3://bucket/team_a/")
            .sweepOptions;

    soft.assertThat(options.createContentKeyFilter()).isPresent();
    soft.assertThat(options.createBaseLocationFilter()).isPresent();
    options.validate();
  }

  @Test
  public void gcCommandAcceptsTargetNamespace() {
    MarkAndSweep gc = parseCommand("gc", "--jdbc-url", "jdbc:foo:", "--target-namespace", "team_a");

    assertThat(gc.sweepOptions.getTargetNamespaces()).containsExactly("team_a");
    assertThat(gc.sweepOptions.createContentKeyFilter().orElseThrow())
        .accepts(ContentKey.of("team_a", "orders"))
        .rejects(ContentKey.of("team_b", "orders"));
  }

  @Test
  public void gcCommandAcceptsStoragePrefix() {
    MarkAndSweep gc =
        parseCommand("gc", "--jdbc-url", "jdbc:foo:", "--storage-prefix", "gs://bucket/team_a/");

    assertThat(gc.sweepOptions.getStoragePrefixes()).containsExactly("gs://bucket/team_a/");
    assertThat(gc.sweepOptions.createBaseLocationFilter().orElseThrow())
        .accepts(StorageUri.of("gs://bucket/team_a/orders/"))
        .rejects(StorageUri.of("s3://bucket/team_a/orders/"));
  }
}
