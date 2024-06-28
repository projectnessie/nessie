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
package org.projectnessie.gc.tool.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import org.projectnessie.gc.tool.cli.CLI;
import org.projectnessie.gc.tool.cli.options.MarkOptions;
import org.projectnessie.model.Branch;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMarkOptions {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void defaultCutoffPolicyNone() {
    MarkLive command = parseCommand("mark", "--jdbc-url", "jdbc:foo:");
    MarkOptions options = command.markOptions;
    assertThat(options.createPerRefCutoffPolicySupplier().get(Branch.of("foo", "12345678")))
        .isEqualTo(CutoffPolicy.NONE);
  }

  @Test
  public void defaultCutoffPolicyNone2() {
    MarkLive command = parseCommand("mark", "--jdbc-url", "jdbc:foo:", "--default-cutoff", "none");
    MarkOptions options = command.markOptions;
    assertThat(options.createPerRefCutoffPolicySupplier().get(Branch.of("foo", "12345678")))
        .isEqualTo(CutoffPolicy.NONE);
  }

  @Test
  public void defaultCutoffPolicyCommits() {
    MarkLive command = parseCommand("mark", "--jdbc-url", "jdbc:foo:", "--default-cutoff", "42");
    MarkOptions options = command.markOptions;
    assertThat(options.createPerRefCutoffPolicySupplier().get(Branch.of("foo", "12345678")))
        .isEqualTo(CutoffPolicy.numCommits(42));
  }

  @Test
  public void defaultCutoffPolicyTimestamp() {
    MarkLive command =
        parseCommand(
            "mark", "--jdbc-url", "jdbc:foo:", "--default-cutoff", "2022-10-06T17:51:00+02:00");
    MarkOptions options = command.markOptions;
    assertThat(options.createPerRefCutoffPolicySupplier().get(Branch.of("foo", "12345678")))
        .isEqualTo(
            CutoffPolicy.atTimestamp(
                ZonedDateTime.of(
                        2022, 10, 6, 17, 51, 0, 0, ZoneId.ofOffset("UTC", ZoneOffset.ofHours(2)))
                    .toInstant()));
  }

  @Test
  public void defaultCutoffPolicyTimestampZ() {
    MarkLive command =
        parseCommand("mark", "--jdbc-url", "jdbc:foo:", "--default-cutoff", "2022-10-06T17:51:00Z");
    MarkOptions options = command.markOptions;
    assertThat(options.createPerRefCutoffPolicySupplier().get(Branch.of("foo", "12345678")))
        .isEqualTo(
            CutoffPolicy.atTimestamp(
                ZonedDateTime.of(
                        2022, 10, 6, 17, 51, 0, 0, ZoneId.ofOffset("UTC", ZoneOffset.ofHours(0)))
                    .toInstant()));
  }

  @Test
  public void defaultCutoffPolicyDuration() {
    MarkLive command =
        parseCommand(
            "mark",
            "--jdbc-url",
            "jdbc:foo:",
            "--cutoff-ref-time",
            "2022-10-06T17:51:00Z",
            "--default-cutoff",
            "PT2H");
    MarkOptions options = command.markOptions;
    assertThat(options.createPerRefCutoffPolicySupplier().get(Branch.of("foo", "12345678")))
        .isEqualTo(
            CutoffPolicy.atTimestamp(
                ZonedDateTime.of(
                        2022, 10, 6, 17, 51, 0, 0, ZoneId.ofOffset("UTC", ZoneOffset.ofHours(0)))
                    .minus(Duration.ofHours(2))
                    .toInstant()));
  }

  @Test
  public void cutoffPoliciesPerRef() {
    MarkLive command =
        parseCommand(
            "mark",
            "--jdbc-url",
            "jdbc:foo:",
            "--default-cutoff",
            "1",
            "--cutoff",
            "main=1000",
            "--cutoff",
            "foo=10,bar=20",
            "--cutoff",
            "zyx.*=42");
    MarkOptions options = command.markOptions;
    PerRefCutoffPolicySupplier cutoffPolicySupplier = options.createPerRefCutoffPolicySupplier();
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("main", null)))
        .isEqualTo(CutoffPolicy.numCommits(1000));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("foo", null)))
        .isEqualTo(CutoffPolicy.numCommits(10));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("bar", null)))
        .isEqualTo(CutoffPolicy.numCommits(20));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("zyx", null)))
        .isEqualTo(CutoffPolicy.numCommits(42));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("zyxzyx", null)))
        .isEqualTo(CutoffPolicy.numCommits(42));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("zyx/mine", null)))
        .isEqualTo(CutoffPolicy.numCommits(42));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("blah", null)))
        .isEqualTo(CutoffPolicy.numCommits(1));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("zyfoo", null)))
        .isEqualTo(CutoffPolicy.numCommits(1));
    soft.assertThat(cutoffPolicySupplier.get(Branch.of("meep", null)))
        .isEqualTo(CutoffPolicy.numCommits(1));
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  static <T extends BaseCommand> T parseCommand(String... args) {
    @SuppressWarnings("InstantiationOfUtilityClass")
    ParseResult parsed = new CommandLine(new CLI(), CommandLine.defaultFactory()).parseArgs(args);
    List<CommandLine> commandLineList = parsed.asCommandLineList();
    CommandLine last = commandLineList.get(commandLineList.size() - 1);
    return last.getCommand();
  }
}
