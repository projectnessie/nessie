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
package org.projectnessie.versioned.transfer.files;

import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractTestExporterImporter<
    EXP extends ExportFileSupplier, IMP extends ImportFileSupplier> {
  @InjectSoftAssertions protected SoftAssertions soft;
  @TempDir protected Path dir;

  protected abstract EXP newExportFileSupplier(String target);

  protected abstract IMP newImportFileSupplier(EXP exporter);

  @ParameterizedTest
  @ValueSource(strings = {"here", "some/folder/between/target"})
  public void exportEmpty(String target) throws Exception {
    EXP exporter = newExportFileSupplier(target);
    exporter.preValidate();
    exporter.close();

    soft.assertThat(exporter.getTargetPath()).isNotNull().exists();

    IMP importer = newImportFileSupplier(exporter);
    importer.close();
  }

  @Test
  public void duplicateFiles() throws Exception {
    try (EXP exporter = newExportFileSupplier("target")) {
      exporter.preValidate();
      try (OutputStream out = exporter.newFileOutput("foo-bar-file")) {
        out.write("a".getBytes(UTF_8));
      }

      soft.assertThatThrownBy(() -> exporter.newFileOutput("foo-bar-file").close())
          .hasMessageContaining("foo-bar-file");
    }
  }

  @Test
  public void noDirectories() throws Exception {
    try (EXP exporter = newExportFileSupplier("target")) {
      exporter.preValidate();

      soft.assertThatThrownBy(() -> exporter.newFileOutput("foo\\file"))
          .hasMessageContaining("Directories not supported");
      soft.assertThatThrownBy(() -> exporter.newFileOutput("foo/file"))
          .hasMessageContaining("Directories not supported");
      soft.assertThatThrownBy(() -> exporter.newFileOutput("foo/bar/file"))
          .hasMessageContaining("Directories not supported");
    }
  }

  @Test
  public void accessFileAndNotFound() throws Exception {
    EXP exporter = newExportFileSupplier("target");
    exporter.preValidate();
    try (OutputStream out = exporter.newFileOutput("data")) {
      out.write("foo/bar/hello".getBytes(UTF_8));
    }
    exporter.close();

    IMP importer = newImportFileSupplier(exporter);

    soft.assertThatThrownBy(() -> importer.newFileInput("does-not-exist"))
        .hasMessageContaining("does-not-exist");

    importer.close();
  }

  static List<byte[]> asByteArrays(String... str) {
    return Arrays.stream(str).map(s -> s.getBytes(UTF_8)).collect(toList());
  }

  @SuppressWarnings("InlineMeInliner")
  static Stream<List<Map.Entry<String, List<byte[]>>>> filesAndContents() {
    List<Map.Entry<String, List<byte[]>>> filesAndContentsSomeTiny = new ArrayList<>();
    filesAndContentsSomeTiny.add(immutableEntry("a", asByteArrays()));
    filesAndContentsSomeTiny.add(immutableEntry("b", asByteArrays("a", "b", "c")));
    filesAndContentsSomeTiny.add(immutableEntry("c", asByteArrays(repeat("a", 1000), "b", "c")));
    filesAndContentsSomeTiny.add(
        immutableEntry("d", asByteArrays(repeat("abc", 1000), repeat("def", 4000), "c")));

    return Stream.of(filesAndContentsSomeTiny);
  }

  @ParameterizedTest
  @MethodSource
  public void filesAndContents(List<Map.Entry<String, List<byte[]>>> entries) throws Exception {
    EXP exporter = newExportFileSupplier("here");
    try (exporter) {
      exporter.preValidate();
      for (Entry<String, List<byte[]>> e : entries) {
        try (OutputStream out = exporter.newFileOutput(e.getKey())) {
          for (byte[] bytes : e.getValue()) {
            out.write(bytes);
          }
        }
      }
    }

    try (IMP importer = newImportFileSupplier(exporter)) {
      for (Map.Entry<String, List<byte[]>> e : entries) {
        byte[] expect = expectedContent(e);
        try (InputStream in = importer.newFileInput(e.getKey())) {
          soft.assertThat(in).hasBinaryContent(expect);
        }
      }
    }

    try (IMP importer = newImportFileSupplier(exporter)) {
      for (int i = entries.size() - 1; i >= 0; i--) {
        Map.Entry<String, List<byte[]>> e = entries.get(i);
        byte[] expect = expectedContent(e);
        try (InputStream in = importer.newFileInput(e.getKey())) {
          soft.assertThat(in).hasBinaryContent(expect);
        }
      }
    }
  }

  private static byte[] expectedContent(Entry<String, List<byte[]>> e) {
    byte[] expect = new byte[e.getValue().stream().mapToInt(a -> a.length).sum()];
    int off = 0;
    for (byte[] bytes : e.getValue()) {
      System.arraycopy(bytes, 0, expect, off, bytes.length);
      off += bytes.length;
    }
    return expect;
  }
}
