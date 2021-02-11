/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.server.providers;

import java.io.IOError;
import java.io.IOException;
import java.util.stream.Stream;

import javax.inject.Singleton;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.AssetKey.NoOpAssetKey;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.ValueWorker;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;
import com.google.protobuf.InvalidProtocolBufferException;

@Singleton
public class TableCommitMetaStoreWorker implements StoreWorker<Contents, CommitMeta> {
  private static final ProtobufMapper PROTOBUF_MAPPER = (ProtobufMapper) new ProtobufMapper()
      .addMixIn(Contents.class, MixIn.class);

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  public interface MixIn {}

  private static ClassValue<ProtobufSchema> SCHEMAS = new ClassValue<ProtobufSchema>() {
    @Override
    protected ProtobufSchema computeValue(Class<?> type) {
      try {
        return PROTOBUF_MAPPER.generateSchemaFor(type);
      } catch (JsonMappingException e) {
        throw new IllegalArgumentException("Cannot generate schema for class " + type, e);
      }
    }
  };

  private final ValueWorker<Contents> tableSerializer = new TableValueWorker();
  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public ValueWorker<Contents> getValueWorker() {
    return tableSerializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  private static class TableValueWorker implements ValueWorker<Contents> {
    private static final String TYPE_PREFIX = "nessie/contents.";

    private final TypeIdResolver typeIdResolver;
    private final TypeIdResolver idTypeResolver;

    private TableValueWorker() {
      final ObjectMapper mapper = new ObjectMapper();
      JavaType baseType = mapper.constructType(Contents.class);
      try {
        typeIdResolver = mapper.getSerializerProviderInstance().findTypeSerializer(baseType).getTypeIdResolver();
        idTypeResolver = mapper.getDeserializationConfig().findTypeDeserializer(baseType).getTypeIdResolver();
      } catch (JsonMappingException e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    public ByteString toBytes(Contents value) {
      final String id = typeIdResolver.idFromValue(value);

      final Output bytes = ByteString.newOutput();
      final JavaType type;
      try {
        type = idTypeResolver.typeFromId(null, id);
        Class<?> contentClass = type.getRawClass();
        ProtobufSchema schema = SCHEMAS.get(contentClass);
        PROTOBUF_MAPPER.writerFor(type).with(schema).writeValue(bytes, value);
      } catch (IOException e) {
        throw new IOError(e);
      }

      return Any.newBuilder()
          .setTypeUrl(TYPE_PREFIX + type)
          .setValue(bytes.toByteString())
          .build()
          .toByteString();
    }

    @Override
    public Contents fromBytes(ByteString bytes) {
      final Any any;
      try {
        any = Any.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(e);
      }
      String typeURL = any.getTypeUrl();
      String typeId = typeURL.substring(TYPE_PREFIX.length());


      try {
        final JavaType type = idTypeResolver.typeFromId(null, typeId);
        return PROTOBUF_MAPPER.readerFor(type).with(SCHEMAS.get(type.getRawClass())).readValue(bytes.newInput());
      } catch (IOException e) {
        throw new IOError(e);
      }
    }

    @Override
    public Stream<AssetKey> getAssetKeys(Contents value) {
      return Stream.of();
    }

    @Override
    public Serializer<AssetKey> getAssetKeySerializer() {
      return NoOpAssetKey.SERIALIZER;
    }
  }

  private static class MetadataSerializer implements Serializer<CommitMeta> {
    private final ProtobufSchema schema = SCHEMAS.get(CommitMeta.class);

    @Override
    public ByteString toBytes(CommitMeta value) {
      final Output bytes = ByteString.newOutput();
      try {
        PROTOBUF_MAPPER.writerFor(CommitMeta.class).with(schema).writeValue(bytes, value);
      } catch (IOException e) {
        throw new IOError(e);
      }
      return bytes.toByteString();
    }

    @Override
    public CommitMeta fromBytes(ByteString bytes) {
      try {
        return PROTOBUF_MAPPER.readerFor(CommitMeta.class).with(schema).readValue(bytes.newInput());
      } catch (IOException e) {
        throw new IOError(e);
      }
    }
  }

}
