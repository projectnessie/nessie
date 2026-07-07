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
package org.projectnessie.versioned.storage.serialize;

import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.contextualReader;
import static org.projectnessie.versioned.storage.common.util.Compressions.compressDefault;
import static org.projectnessie.versioned.storage.common.util.Compressions.uncompress;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.dataformat.smile.SmileMapper;

public final class SmileSerialization {

  private static final ObjectMapper SMILE_MAPPER =
      SmileMapper.builder()
          .enable(MapperFeature.DEFAULT_VIEW_INCLUSION)
          .polymorphicTypeValidator(
              BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build())
          .findAndAddModules()
          .build();
  private static final ObjectWriter SMILE_WRITER = SMILE_MAPPER.writerWithView(StorageView.class);

  private SmileSerialization() {}

  public static Obj deserializeObj(
      ObjId id,
      String versionToken,
      byte[] data,
      ObjType type,
      long referenced,
      Compression compression) {
    ObjectReader reader = contextualReader(SMILE_MAPPER, type, id, versionToken, referenced);
    data = uncompress(compression, data);
    return reader.readValue(data);
  }

  public static Obj deserializeObj(
      ObjId id,
      String versionToken,
      byte[] data,
      ObjType type,
      long referenced,
      String compression) {
    return deserializeObj(
        id, versionToken, data, type, referenced, Compression.fromValue(compression));
  }

  public static Obj deserializeObj(
      ObjId id,
      String versionToken,
      ByteBuffer data,
      ObjType type,
      long referenced,
      Compression compression) {
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    return deserializeObj(id, versionToken, bytes, type, referenced, compression);
  }

  public static Obj deserializeObj(
      ObjId id,
      String versionToken,
      ByteBuffer data,
      ObjType type,
      long referenced,
      String compression) {
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    return deserializeObj(id, versionToken, bytes, type, referenced, compression);
  }

  public static byte[] serializeObj(Obj obj, Consumer<Compression> compression) {
    return compressDefault(SMILE_WRITER.writeValueAsBytes(obj), compression);
  }
}
