/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.contentgenerator.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

@ExtendWith(SoftAssertionsExtension.class)
public class TestInvalidOptions {

  @InjectSoftAssertions protected SoftAssertions soft;

  public static Stream<Arguments> committingCommands() {
    return Stream.of(
        Arguments.of(
            new String[] {"delete", "--all", "--branch", "main"},
            "--all, --branch=<ref> are mutually exclusive"),
        Arguments.of(
            new String[] {"delete", "--recursive", "--branch", "main"},
            "Missing required argument(s): --key=<key element>"),
        Arguments.of(
            new String[] {
              "delete", "--key", "a", "--input", "/tmp/file.csv", "--format", "CSV_KEYS"
            },
            "[-k=<key element> [-k=<key element>]... [-R]] and [-f=<input> -F=<format>"
                + " [-S=<separator>]] are mutually exclusive "),
        Arguments.of(
            new String[] {"delete", "--format", "CONTENT_INFO_JSON"},
            "Missing required argument(s): --input=<input>"),
        Arguments.of(
            new String[] {"content-refresh", "--all", "--branch", "main"},
            "--all, --branch=<ref> are mutually exclusive"),
        Arguments.of(
            new String[] {"content-refresh", "--recursive", "--branch", "main"},
            "Missing required argument(s): --key=<key element>"),
        Arguments.of(
            new String[] {
              "content-refresh", "--key", "a", "--input", "/tmp/file.csv", "--format", "CSV_KEYS"
            },
            "[-k=<key element> [-k=<key element>]... [-R]] and [-f=<input> -F=<format>"
                + " [-S=<separator>]] are mutually exclusive "),
        Arguments.of(
            new String[] {"content-refresh", "--format", "CONTENT_INFO_JSON"},
            "Missing required argument(s): --input=<input>"));
  }

  public static Stream<Arguments> positiveCounts() {
    return Stream.of(
        Arguments.of((Object) new String[] {"generate", "-n", "-22"}),
        Arguments.of((Object) new String[] {"generate", "--num-commits", "0"}),
        Arguments.of((Object) new String[] {"generate", "--num-tables", "-1"}),
        Arguments.of(
            (Object)
                new String[] {"copy", "--num-copies", "0", "--from", "source", "--to", "copy-%d"}));
  }

  @ParameterizedTest
  @MethodSource
  public void positiveCounts(String[] args) {
    StringWriter stdout = new StringWriter();
    StringWriter stderr = new StringWriter();
    NessieContentGenerator generator =
        new NessieContentGenerator() {
          @Override
          public NessieApiV2 createNessieApiInstance() {
            throw new AssertionError("Option validation must happen before connecting to Nessie");
          }
        };

    int exitCode =
        NessieContentGenerator.runMain(
            generator, new PrintWriter(stdout), new PrintWriter(stderr), args);

    assertThat(exitCode).isEqualTo(2);
    assertThat(stdout.toString()).doesNotContain("Done creating contents.");
    assertThat(stderr.toString()).contains("Invalid value for option", "value must be at least 1");
  }

  @ParameterizedTest
  @MethodSource
  public void committingCommands(String[] args, String error) {
    ProcessResult result = runGeneratorCmd(args);
    soft.assertThat(result.getExitCode()).isEqualTo(2);
    soft.assertThat(result.getStdErrLines())
        .satisfiesOnlyOnce(line -> soft.assertThat(line).contains(error));
  }
}
