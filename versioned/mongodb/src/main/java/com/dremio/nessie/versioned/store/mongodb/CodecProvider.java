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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.Arrays;
import java.util.Map;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.impl.PersistentBase;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableMap;

/**
 * Provider for codecs that encode/decode Nessie Entities.
 */
final class CodecProvider implements org.bson.codecs.configuration.CodecProvider {

  /**
   * Codec responsible for the encoding and decoding of entities to a BSON objects.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static class EntityCodec<C> implements Codec<C> {
    private final ValueType valueType;

    /**
     * Constructor.
     * @param valueType type handled by this codec
     */
    EntityCodec(ValueType valueType) {
      this.valueType = valueType;
    }

    /**
     * This deserializes a BSON stream to create an L1 object.
     * @param bsonReader that provides the BSON
     * @param decoderContext not used
     * @return the object created from the BSON stream.
     */
    @Override
    public C decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return valueType.buildEntity(consumer -> MongoSerDe.produceToConsumer(bsonReader, valueType, consumer));
    }

    /**
     * This serializes an object into a BSON stream. The serialization is delegated to
     * a concrete {@link MongoConsumer} implementation via {@link PersistentBase#applyToConsumer(BaseConsumer)}.
     * @param bsonWriter that encodes each attribute to BSON
     * @param obj the object to encode
     * @param encoderContext not used
     */
    @Override
    public void encode(BsonWriter bsonWriter, C obj, EncoderContext encoderContext) {
      bsonWriter.writeStartDocument();
      MongoConsumer<?> mongoConsumer = MongoSerDe.newMongoConsumer(valueType, bsonWriter);
      PersistentBase memoizedId = (PersistentBase) obj;
      memoizedId.applyToConsumer(mongoConsumer);
      mongoConsumer.build();
      bsonWriter.writeEndDocument();
    }

    /**
     * A getter of the class being encoded.
     * @return the class being encoded
     */
    @Override
    public Class<C> getEncoderClass() {
      return (Class<C>) valueType.getObjectClass();
    }
  }

  private static class IdCodec implements Codec<Id> {
    @Override
    public Id decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return Id.of(bsonReader.readBinaryData().getData());
    }

    @Override
    public void encode(BsonWriter bsonWriter, Id id, EncoderContext encoderContext) {
      bsonWriter.writeBinaryData(new BsonBinary(id.toBytes()));
    }

    @Override
    public Class<Id> getEncoderClass() {
      return Id.class;
    }
  }

  static final IdCodec ID_CODEC_INSTANCE = new IdCodec();

  private static final Map<Class<?>, Codec<?>> CODECS;

  static {
    final ImmutableMap.Builder<Class<?>, Codec<?>> builder = ImmutableMap.builder();
    Arrays.stream(ValueType.values()).forEach(v -> builder.put(v.getObjectClass(), new EntityCodec<>(v)));

    // Specific case where the ID is not encoded as a document, but directly as a binary value. Keep within this provider
    // as Mongo appears to rely on the same provider for all related codecs, and splitting this to a separate provider
    // results in the incorrect codec being used.
    builder.put(Id.class, ID_CODEC_INSTANCE);
    CODECS = builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
    final Codec<T> codec = (Codec<T>)CODECS.get(clazz);
    if (null != codec) {
      return codec;
    }

    // In most cases, the codec for a class is directly entered, but also account for when there are subclasses for
    // a registered class and get the CODEC for that.
    for (Map.Entry<Class<?>, Codec<?>> entry : CODECS.entrySet()) {
      if (entry.getKey().isAssignableFrom(clazz)) {
        return (Codec<T>)entry.getValue();
      }
    }

    return null;
  }
}
