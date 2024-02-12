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
package org.projectnessie.versioned.storage.common.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.projectnessie.versioned.storage.common.util.Compressions.KEEP_UNCOMPRESSED;

import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.objtypes.Compression;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCompressions {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 40, KEEP_UNCOMPRESSED - 1, KEEP_UNCOMPRESSED})
  public void keepUncompressed(int len) {
    byte[] data = ("x".repeat(len)).getBytes(UTF_8);
    AtomicReference<Compression> compression = new AtomicReference<>();
    byte[] compressed = Compressions.compressDefault(data, compression::set);
    soft.assertThat(compressed).isSameAs(data);
    soft.assertThat(compression.get()).isSameAs(Compression.NONE);
  }

  @ParameterizedTest
  @ValueSource(ints = {KEEP_UNCOMPRESSED + 1, 32768})
  public void doCompress(int len) {
    byte[] data = ("x".repeat(len)).getBytes(UTF_8);
    AtomicReference<Compression> compression = new AtomicReference<>();
    byte[] compressed = Compressions.compressDefault(data, compression::set);
    soft.assertThat(compressed.length).isLessThan(data.length);
    soft.assertThat(compression.get()).isSameAs(Compression.GZIP);
  }

  @ParameterizedTest
  @EnumSource(
      value = Compression.class,
      names = {"NONE", "SNAPPY", "DEFLATE", "GZIP"})
  public void supportedCompression(Compression compression) {
    byte[] data = ("x".repeat(10)).getBytes(UTF_8);
    byte[] compressed = Compressions.compress(compression, data);
    byte[] uncompressed = Compressions.uncompress(compression, compressed);
    soft.assertThat(uncompressed).containsExactly(data);
  }

  @ParameterizedTest
  @EnumSource(value = Compression.class)
  public void unsupportedCompression(Compression compression) {
    assumeThat(compression)
        .isNotIn(Compression.NONE, Compression.SNAPPY, Compression.DEFLATE, Compression.GZIP);
    byte[] data = ("x".repeat(10)).getBytes(UTF_8);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Compressions.compress(compression, data))
        .withMessage("Compression %s not implemented", compression.name());
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Compressions.uncompress(compression, data))
        .withMessage("Compression %s not implemented", compression.name());
  }
}
