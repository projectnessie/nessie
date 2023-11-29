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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.projectnessie.versioned.storage.common.json.ObjIdHelper;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class SmileSerialization {

  private static final ObjectMapper SMILE_MAPPER = new SmileMapper().findAndRegisterModules();

  private SmileSerialization() {}

  public static Obj deserializeObj(ObjId id, byte[] data, String targetClassFqdn) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Obj> targetClass = (Class<? extends Obj>) Class.forName(targetClassFqdn);
      return deserializeObj(id, data, targetClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static Obj deserializeObj(ObjId id, ByteBuffer data, String targetClassFqdn) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Obj> targetClass = (Class<? extends Obj>) Class.forName(targetClassFqdn);
      return deserializeObj(id, data, targetClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static Obj deserializeObj(ObjId id, byte[] data, Class<? extends Obj> targetClass) {
    try {
      ObjectReader reader = ObjIdHelper.storeObjIdInContext(SMILE_MAPPER, id);
      return reader.readValue(data, targetClass);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Obj deserializeObj(ObjId id, ByteBuffer data, Class<? extends Obj> targetClass) {
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    return deserializeObj(id, bytes, targetClass);
  }

  public static byte[] serializeObj(Obj obj) {
    try {
      return SMILE_MAPPER.writeValueAsBytes(obj);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
