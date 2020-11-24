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
package com.dremio.nessie.versioned.impl;

import java.util.HashMap;
import java.util.Map;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.mongodb.codecs.EntityCodec;

/**
 * Codecs provide the heart of the SerDe process to/from BSON format.
 * The Codecs are inserted into a CodecRegistry. However they may require the CodecRegistry to do their job.
 * This apparent two way interdependency is resolved by using a CodecProvider.
 * The CodecProvider is a factory for Codecs.
 */
@SuppressWarnings("unchecked")
public class CodecProvider implements org.bson.codecs.configuration.CodecProvider {
  static class IdCodec implements Codec<Id> {
    @Override
    public Id decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return Id.of(bsonReader.readBinaryData().getData());
    }

    @Override
    public void encode(BsonWriter bsonWriter, Id id, EncoderContext encoderContext) {
      bsonWriter.writeBinaryData(new BsonBinary(id.getValue().toByteArray()));
    }

    @Override
    public Class<Id> getEncoderClass() {
      return Id.class;
    }
  }

  private static final Map<Class<?>, Codec<?>> CODECS = new HashMap<>();

  static {
    CODECS.put(L1.class, new EntityCodec(L1.class, L1.SCHEMA));
    CODECS.put(L2.class, new EntityCodec(L2.class, L2.SCHEMA));
    CODECS.put(L3.class, new EntityCodec(L3.class, L3.SCHEMA));
    CODECS.put(Fragment.class, new EntityCodec(Fragment.class, Fragment.SCHEMA));
    CODECS.put(InternalRef.class, new EntityCodec(InternalRef.class, InternalRef.SCHEMA));
    CODECS.put(InternalCommitMetadata.class, new EntityCodec(InternalCommitMetadata.class, InternalCommitMetadata.SCHEMA));
    CODECS.put(InternalValue.class, new EntityCodec(InternalValue.class, InternalValue.SCHEMA));
    CODECS.put(Id.class, new IdCodec());
    CODECS.put(InternalBranch.class, new EntityCodec(InternalBranch.class, InternalRef.SCHEMA));
    CODECS.put(InternalTag.class, new EntityCodec(InternalTag.class, InternalRef.SCHEMA));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
    return (Codec<T>)CODECS.get(clazz);
  }
}
