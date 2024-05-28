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
package org.projectnessie.catalog.formats.iceberg.manifest;

import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION_DEFAULT;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION_LEVEL;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.AVRO_COMPRESSION_LEVEL_DEFAULT;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.DELETE_AVRO_COMPRESSION;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.DELETE_AVRO_COMPRESSION_LEVEL;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;

final class AvroSerializationContext {

  void applyToDataFileWriter(DataFileWriter<?> entryWriter) {
    entryWriter.setCodec(codec());
    syncInterval().ifPresent(entryWriter::setSyncInterval);
    flushOnEveryBlock().ifPresent(entryWriter::setFlushOnEveryBlock);
  }

  private enum Codec {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    ZSTD
  }

  private static final int ZSTD_COMPRESSION_LEVEL_DEFAULT = 1;
  private static final int GZIP_COMPRESSION_LEVEL_DEFAULT = 9;

  private final CodecFactory codec;

  private AvroSerializationContext(CodecFactory codec) {
    this.codec = codec;
  }

  static AvroSerializationContext dataSerializationContext(Map<String, String> config) {
    String codecAsString = config.getOrDefault(AVRO_COMPRESSION, AVRO_COMPRESSION_DEFAULT);
    String compressionLevel =
        config.getOrDefault(AVRO_COMPRESSION_LEVEL, AVRO_COMPRESSION_LEVEL_DEFAULT);
    CodecFactory codec = toCodec(codecAsString, compressionLevel);

    return new AvroSerializationContext(codec);
  }

  static AvroSerializationContext deleteSerializationContext(Map<String, String> config) {
    String codecAsString = config.get(DELETE_AVRO_COMPRESSION);
    String compressionLevel =
        config.getOrDefault(DELETE_AVRO_COMPRESSION_LEVEL, AVRO_COMPRESSION_LEVEL_DEFAULT);
    CodecFactory codec =
        codecAsString != null
            ? toCodec(codecAsString, compressionLevel)
            // default delete config using data config
            : dataSerializationContext(config).codec();

    return new AvroSerializationContext(codec);
  }

  private static CodecFactory toCodec(String codecAsString, String compressionLevel) {
    CodecFactory codecFactory;
    try {
      switch (Codec.valueOf(codecAsString.toUpperCase(Locale.ENGLISH))) {
        case UNCOMPRESSED:
          codecFactory = CodecFactory.nullCodec();
          break;
        case SNAPPY:
          codecFactory = CodecFactory.snappyCodec();
          break;
        case ZSTD:
          codecFactory =
              CodecFactory.zstandardCodec(
                  compressionLevelAsInt(compressionLevel, ZSTD_COMPRESSION_LEVEL_DEFAULT));
          break;
        case GZIP:
          codecFactory =
              CodecFactory.deflateCodec(
                  compressionLevelAsInt(compressionLevel, GZIP_COMPRESSION_LEVEL_DEFAULT));
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec: " + codecAsString);
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported compression codec: " + codecAsString);
    }
    return codecFactory;
  }

  private static int compressionLevelAsInt(
      String tableCompressionLevel, int defaultCompressionLevel) {
    return tableCompressionLevel != null
        ? Integer.parseInt(tableCompressionLevel)
        : defaultCompressionLevel;
  }

  CodecFactory codec() {
    return codec;
  }

  OptionalInt syncInterval() {
    return OptionalInt.empty();
  }

  Optional<Boolean> flushOnEveryBlock() {
    return Optional.empty();
  }
}
