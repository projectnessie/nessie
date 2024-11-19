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
package org.projectnessie.versioned.transfer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Strings;
import jakarta.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.AbstractMessage;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.NamedReference;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSizeLimitedOutput {
  @InjectSoftAssertions protected SoftAssertions soft;

  static SizeLimitedOutput newSizeLimitedOutput(
      NessieExporter exporter,
      Function<String, OutputStream> outputSupplier,
      Consumer<String> fileNames,
      LongConsumer finalCount,
      String prefix) {
    ExportFileSupplier exportFiles =
        new ExportFileSupplier() {
          @Nonnull
          @Override
          public Path getTargetPath() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void preValidate() {}

          @Nonnull
          @Override
          public OutputStream newFileOutput(@Nonnull String fileName) {
            return outputSupplier.apply(fileName);
          }

          @Override
          public long fixMaxFileSize(long userProvidedMaxFileSize) {
            return userProvidedMaxFileSize;
          }

          @Override
          public void close() {}
        };

    return new SizeLimitedOutput(exportFiles, exporter, prefix, fileNames, finalCount);
  }

  @Test
  public void noData() throws Exception {
    Function<String, OutputStream> outputSupplier = x -> null;
    List<String> files = new ArrayList<>();
    AtomicLong count = new AtomicLong(-1L);

    NessieExporter exporter = mockExporter(1000, 100);

    SizeLimitedOutput sizeLimitedOutput =
        newSizeLimitedOutput(exporter, outputSupplier, files::add, count::set, "prefix");

    verify(exporter, times(1)).maxFileSize();
    verify(exporter, times(1)).outputBufferSize();

    soft.assertThat(count).hasValue(-1);

    sizeLimitedOutput.finish();

    soft.assertThat(files).isEmpty();
    soft.assertThat(count).hasValue(0);
  }

  private static NessieExporter mockExporter(long maxFileSize, int outputBufferSize) {
    NessieExporter exporter = mock(NessieExporter.class);
    when(exporter.maxFileSize()).thenReturn(maxFileSize);
    when(exporter.outputBufferSize()).thenReturn(outputBufferSize);
    return exporter;
  }

  @SuppressWarnings("InlineMeInliner")
  static Stream<AbstractMessage> singleMessageSingleFile() {
    return Stream.of(
        ExportMeta.getDefaultInstance(),
        NamedReference.newBuilder().setName(Strings.repeat("a", 10_000)).build());
  }

  @ParameterizedTest
  @MethodSource
  public void singleMessageSingleFile(AbstractMessage msg) throws Exception {
    List<ByteArrayOutputStream> outputs = new ArrayList<>();
    Function<String, OutputStream> outputSupplier =
        x -> {
          ByteArrayOutputStream output = new ByteArrayOutputStream();
          outputs.add(output);
          return output;
        };
    List<String> files = new ArrayList<>();
    AtomicLong entityCount = new AtomicLong(-1L);

    NessieExporter exporter = mockExporter(1000, 100);

    SizeLimitedOutput sizeLimitedOutput =
        newSizeLimitedOutput(exporter, outputSupplier, files::add, entityCount::set, "prefix");

    sizeLimitedOutput.writeEntity(msg);

    sizeLimitedOutput.finish();

    soft.assertThat(files).containsExactly("prefix-00000001");
    soft.assertThat(entityCount).hasValue(1);
    soft.assertThat(outputs).hasSize(1);
  }

  @Test
  @SuppressWarnings("InlineMeInliner")
  public void twoMessagesTwoFiles() throws Exception {
    AbstractMessage msg = NamedReference.newBuilder().setName(Strings.repeat("a", 900)).build();

    List<ByteArrayOutputStream> outputs = new ArrayList<>();
    Function<String, OutputStream> outputSupplier =
        x -> {
          ByteArrayOutputStream output = new ByteArrayOutputStream();
          outputs.add(output);
          return output;
        };
    List<String> files = new ArrayList<>();
    AtomicLong entityCount = new AtomicLong(-1L);

    NessieExporter exporter = mockExporter(1000, 100);

    SizeLimitedOutput sizeLimitedOutput =
        newSizeLimitedOutput(exporter, outputSupplier, files::add, entityCount::set, "prefix");

    sizeLimitedOutput.writeEntity(msg);
    sizeLimitedOutput.writeEntity(msg);

    sizeLimitedOutput.finish();

    soft.assertThat(files).containsExactly("prefix-00000001", "prefix-00000002");
    soft.assertThat(entityCount).hasValue(2);
    soft.assertThat(outputs).hasSize(2);
  }

  @Test
  @SuppressWarnings("InlineMeInliner")
  public void twoMessagesOneFile() throws Exception {
    AbstractMessage msg = NamedReference.newBuilder().setName(Strings.repeat("a", 900)).build();

    List<ByteArrayOutputStream> outputs = new ArrayList<>();
    Function<String, OutputStream> outputSupplier =
        x -> {
          ByteArrayOutputStream output = new ByteArrayOutputStream();
          outputs.add(output);
          return output;
        };
    List<String> files = new ArrayList<>();
    AtomicLong entityCount = new AtomicLong(-1L);

    NessieExporter exporter = mockExporter(Long.MAX_VALUE, 100);

    SizeLimitedOutput sizeLimitedOutput =
        newSizeLimitedOutput(exporter, outputSupplier, files::add, entityCount::set, "prefix");

    sizeLimitedOutput.writeEntity(msg);
    sizeLimitedOutput.writeEntity(msg);

    sizeLimitedOutput.finish();

    soft.assertThat(files).containsExactly("prefix-00000001");
    soft.assertThat(entityCount).hasValue(2);
    soft.assertThat(outputs).hasSize(1);
  }
}
