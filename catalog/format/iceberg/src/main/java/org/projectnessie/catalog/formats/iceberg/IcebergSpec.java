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
package org.projectnessie.catalog.formats.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.projectnessie.catalog.formats.iceberg.manifest.Avro;
import org.projectnessie.catalog.formats.iceberg.manifest.AvroBundle;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;

public enum IcebergSpec {
  V1(IcebergSpecV1.class, 1),
  V2(IcebergSpecV2.class, 2);

  private final Class<?> jsonView;
  private final int version;

  IcebergSpec(Class<?> jsonView, int version) {
    this.jsonView = jsonView;
    this.version = version;
  }

  public Class<?> jsonView() {
    return jsonView;
  }

  public int version() {
    return version;
  }

  public ObjectReader jsonReader() {
    if (jsonView == IcebergSpecV1.class) {
      return IcebergJsonReadersWriters.readerV1;
    }
    if (jsonView == IcebergSpecV2.class) {
      return IcebergJsonReadersWriters.readerV2;
    }
    return IcebergJson.objectMapper().readerWithView(jsonView());
  }

  public ObjectWriter jsonWriter() {
    if (jsonView == IcebergSpecV1.class) {
      return IcebergJsonReadersWriters.writerV1;
    }
    if (jsonView == IcebergSpecV2.class) {
      return IcebergJsonReadersWriters.writerV2;
    }
    return IcebergJson.objectMapper().writerWithView(jsonView());
  }

  public AvroBundle avroBundle() {
    return Avro.bundleFor(version());
  }

  public static IcebergSpec forVersion(int version) {
    switch (version) {
      case 1:
        return V1;
      case 2:
        return V2;
      default:
        throw new IllegalArgumentException("Unkown Iceberg spec version " + version);
    }
  }

  public static final class IcebergSpecV1 {
    private IcebergSpecV1() {}
  }

  public static final class IcebergSpecV2 {
    private IcebergSpecV2() {}
  }

  private static final class IcebergJsonReadersWriters {
    static final ObjectReader readerV1;
    static final ObjectReader readerV2;
    static final ObjectWriter writerV1;
    static final ObjectWriter writerV2;

    static {
      ObjectMapper mapper = IcebergJson.objectMapper();
      readerV1 = mapper.readerWithView(IcebergSpecV1.class);
      readerV2 = mapper.readerWithView(IcebergSpecV2.class);
      writerV1 = mapper.writerWithView(IcebergSpecV1.class);
      writerV2 = mapper.writerWithView(IcebergSpecV2.class);
    }
  }
}
