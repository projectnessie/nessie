/*
 * Copyright (C) 2026 Dremio
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

import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_ID_KEY;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_REFERENCED_KEY;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_TYPE_KEY;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_VERS_KEY;
import static org.projectnessie.versioned.storage.common.objtypes.UpdateableObj.extractVersionToken;
import static org.projectnessie.versioned.storage.common.persist.ObjTypes.objTypeByName;
import static org.projectnessie.versioned.storage.common.util.Compressions.compressDefault;
import static org.projectnessie.versioned.storage.common.util.Compressions.uncompress;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.InvalidProtocolBufferException;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.CompressionProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.CustomProto;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.ObjProto;

final class LegacyJackson2ObjSerialization {
  private static final ObjectMapper SMILE_MAPPER =
      new SmileMapper()
          .registerModule(legacyPersistModule())
          .registerModule(new GuavaModule())
          .registerModule(new Jdk8Module())
          .registerModule(new JavaTimeModule())
          .addMixIn(TaskState.class, LegacyTaskState.class);
  private static final ObjectWriter SMILE_WRITER = SMILE_MAPPER.writerWithView(StorageView.class);

  private LegacyJackson2ObjSerialization() {}

  private static SimpleModule legacyPersistModule() {
    SimpleModule module = new SimpleModule();
    module.addSerializer(new LegacyObjIdSerializer());
    module.addDeserializer(ObjId.class, new LegacyObjIdDeserializer());
    return module;
  }

  static byte[] serializeObj(Obj obj, boolean includeVersionToken) throws ObjTooLargeException {
    if (obj.type() instanceof StandardObjType) {
      return ProtoSerialization.serializeObj(
          obj, Integer.MAX_VALUE, Integer.MAX_VALUE, includeVersionToken);
    }

    ObjProto.Builder builder = ObjProto.newBuilder().setReferenced(obj.referenced());
    return builder.setCustom(serializeCustom(obj, includeVersionToken)).build().toByteArray();
  }

  static Obj deserializeObj(ObjId id, long referenced, byte[] serialized, String versionToken) {
    try {
      ObjProto proto = ObjProto.parseFrom(serialized);
      if (!proto.hasCustom()) {
        return ProtoSerialization.deserializeObj(id, referenced, serialized, versionToken);
      }

      if (referenced == 0L) {
        referenced = proto.getReferenced();
      }
      CustomProto custom = proto.getCustom();
      ObjType type = objTypeByName(custom.getObjType());
      if (versionToken == null && custom.hasVersionToken()) {
        versionToken = custom.getVersionToken();
      }

      return deserializeSmile(
          id,
          versionToken,
          custom.getData().toByteArray(),
          type,
          referenced,
          Compression.valueOf(custom.getCompression().name()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  static byte[] serializeSmile(Obj obj, CompressionHolder compression) {
    try {
      return compressDefault(SMILE_WRITER.writeValueAsBytes(obj), compression::set);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Obj deserializeSmile(
      ObjId id,
      String versionToken,
      byte[] data,
      ObjType type,
      long referenced,
      Compression compression) {
    ObjectReader reader = contextualReader(type, id, versionToken, referenced);
    try {
      return reader.readValue(uncompress(compression, data), type.targetClass());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static CustomProto.Builder serializeCustom(Obj obj, boolean includeVersionToken) {
    CustomProto.Builder builder = CustomProto.newBuilder().setObjType(obj.type().shortName());
    if (includeVersionToken) {
      extractVersionToken(obj).ifPresent(builder::setVersionToken);
    }
    byte[] bytes =
        serializeSmile(
            obj,
            compression -> builder.setCompression(CompressionProto.valueOf(compression.name())));
    builder.setData(ByteString.copyFrom(bytes));
    return builder;
  }

  private static ObjectReader contextualReader(
      ObjType objType, ObjId id, String versionToken, long referenced) {
    InjectableValues values =
        new InjectableValues.Std()
            .addValue(OBJ_ID_KEY, id)
            .addValue(OBJ_VERS_KEY, versionToken)
            .addValue(OBJ_TYPE_KEY, objType)
            .addValue(OBJ_REFERENCED_KEY, referenced);
    return SMILE_MAPPER.reader(values).forType(objType.targetClass());
  }

  interface CompressionHolder {
    void set(Compression compression);
  }

  private static final class LegacyObjIdSerializer
      extends com.fasterxml.jackson.databind.ser.std.StdSerializer<ObjId> {
    private LegacyObjIdSerializer() {
      super(ObjId.class);
    }

    @Override
    public void serialize(
        ObjId value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider provider)
        throws IOException {
      gen.writeBinary(value.asByteArray());
    }
  }

  private static final class LegacyObjIdDeserializer
      extends com.fasterxml.jackson.databind.deser.std.StdDeserializer<ObjId> {
    private LegacyObjIdDeserializer() {
      super(ObjId.class);
    }

    @Override
    public ObjId deserialize(
        com.fasterxml.jackson.core.JsonParser p,
        com.fasterxml.jackson.databind.DeserializationContext ctx)
        throws IOException {
      return ObjId.objIdFromByteArray(p.getBinaryValue());
    }
  }

  @SuppressWarnings("UnusedMethod")
  private interface LegacyTaskState {
    @JsonSerialize(using = LegacyInstantAsLongSerializer.class)
    Instant retryNotBefore();

    @JsonSerialize(using = LegacyInstantAsLongSerializer.class)
    Instant lostNotBefore();
  }

  private static final class LegacyInstantAsLongSerializer extends StdSerializer<Instant> {
    private LegacyInstantAsLongSerializer() {
      super(Instant.class);
    }

    @Override
    public void serialize(
        Instant value, com.fasterxml.jackson.core.JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (value == null) {
        gen.writeNull();
      } else {
        gen.writeNumber(value.toEpochMilli());
      }
    }
  }
}
