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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public final class SmileSerialization {

  private static final ObjectMapper SMILE_MAPPER = new SmileMapper().findAndRegisterModules();
  private static final ObjectWriter SMILE_WRITER = SMILE_MAPPER.writerWithView(StorageView.class);

  private SmileSerialization() {}

  public static Obj deserializeObj(
      ObjId id,
      String versionToken,
      byte[] data,
      ObjType type,
      long referenced,
      Compression compression) {
    try {
      ObjectReader reader = contextualReader(SMILE_MAPPER, type, id, versionToken, referenced);
      data = uncompress(compression, data);
      return reader.readValue(data, type.targetClass());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
    try {
      return compressDefault(SMILE_WRITER.writeValueAsBytes(obj), compression);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
