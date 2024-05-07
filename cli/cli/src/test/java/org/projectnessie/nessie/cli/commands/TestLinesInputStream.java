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
package org.projectnessie.nessie.cli.commands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestLinesInputStream {
  @ParameterizedTest
  @MethodSource("sources")
  public void readAllBytes(Supplier<Stream<String>> sourceSupplier) throws IOException {
    Stream<String> source = sourceSupplier.get();

    byte[] data = new LinesInputStream(source).readAllBytes();

    String ref = sourceSupplier.get().map(s -> s + '\n').collect(Collectors.joining());

    assertThat(new String(data, UTF_8)).isEqualTo(ref);
  }

  @ParameterizedTest
  @MethodSource("sources")
  public void read(Supplier<Stream<String>> sourceSupplier) throws IOException {
    Stream<String> source = sourceSupplier.get();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (LinesInputStream in = new LinesInputStream(source)) {
      while (true) {
        int rd = in.read();
        if (rd == -1) {
          break;
        }
        out.write(rd);
      }
    }

    String ref = sourceSupplier.get().map(s -> s + '\n').collect(Collectors.joining());

    assertThat(out.toString(UTF_8)).isEqualTo(ref);
  }

  @ParameterizedTest
  @MethodSource("sources")
  public void read13(Supplier<Stream<String>> sourceSupplier) throws IOException {
    Stream<String> source = sourceSupplier.get();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] buf = new byte[13];
    try (LinesInputStream in = new LinesInputStream(source)) {

      while (true) {
        int rd = in.read(buf);
        if (rd == -1) {
          break;
        }
        out.write(buf, 0, rd);
      }
    }

    String ref = sourceSupplier.get().map(s -> s + '\n').collect(Collectors.joining());

    assertThat(out.toString(UTF_8)).isEqualTo(ref);
  }

  @ParameterizedTest
  @MethodSource("sources")
  public void read13mid(Supplier<Stream<String>> sourceSupplier) throws IOException {
    Stream<String> source = sourceSupplier.get();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] buf = new byte[26];
    try (LinesInputStream in = new LinesInputStream(source)) {

      while (true) {
        int rd = in.read(buf, 5, 5 + 13);
        if (rd == -1) {
          break;
        }
        out.write(buf, 5, rd);
      }
    }

    String ref = sourceSupplier.get().map(s -> s + '\n').collect(Collectors.joining());

    assertThat(out.toString(UTF_8)).isEqualTo(ref);
  }

  static Stream<Supplier<Stream<String>>> sources() {
    return Stream.of(
        () -> IntStream.range(0, 1000).mapToObj(i -> ""),
        () -> IntStream.range(0, 1000).mapToObj(i -> "line #" + i),
        () -> IntStream.range(0, 1000).mapToObj(i -> "line #" + i + " " + "1234567890".repeat(10)));
  }
}
