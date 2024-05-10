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
package org.projectnessie.catalog.formats.iceberg.meta;

public final class IcebergTableProperties {
  private IcebergTableProperties() {}

  public static final String AVRO_COMPRESSION = "write.avro.compression-codec";
  public static final String DELETE_AVRO_COMPRESSION = "write.delete.avro.compression-codec";
  public static final String AVRO_COMPRESSION_DEFAULT = "gzip";

  public static final String AVRO_COMPRESSION_LEVEL = "write.avro.compression-level";
  public static final String DELETE_AVRO_COMPRESSION_LEVEL = "write.delete.avro.compression-level";
  public static final String AVRO_COMPRESSION_LEVEL_DEFAULT = null;

  /**
   * Iceberg truncates per-column statistics in manifest-files for variable size types (strings,
   * blobs) to this length by default.
   *
   * <p>See {@code org.apache.iceberg.TableProperties#DEFAULT_WRITE_METRICS_MODE_DEFAULT}.
   */
  public static final int WRITE_METRICS_DEFAULT_TRUNCATION = 16;

  /**
   * The default number of columns for which Iceberg includes statistics in metadata-files.
   *
   * <p>See {@code org.apache.iceberg.TableProperties#METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT}
   */
  public static final int WRITE_METRICS_MAX_INFERRED_COLUMNS = 100;

  /**
   * Split size applied when <em>rewriting</em> files. This value does not prevent you from
   * generating (new) manifest-files that are bigger.).
   *
   * <p>See {@code org.apache.iceberg.TableProperties#METADATA_SPLIT_SIZE}.
   */
  public static final long METADATA_SPLIT_SIZE_DEFAULT = 32 * 1024 * 1024; // 32 MB
}
