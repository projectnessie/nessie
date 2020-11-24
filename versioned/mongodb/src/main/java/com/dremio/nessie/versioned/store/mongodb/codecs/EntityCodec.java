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
package com.dremio.nessie.versioned.store.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.dremio.nessie.versioned.store.SimpleSchema;

/**
 * Codec responsible for the encoding and decoding of Entities to a BSON objects.
 */
public class EntityCodec<C, S> implements Codec<C> {
  private final Class<C> clazz;
  private final SimpleSchema<S> schema;

  /**
   * Constructor.
   * @param clazz the class type to encode/decode.
   * @param schema the schema of the class.
   */
  public EntityCodec(Class<C> clazz, SimpleSchema<S> schema) {
    this.clazz = clazz;
    this.schema = schema;
  }

  /**
   * This deserializes a BSON stream to create an L1 object.
   * @param bsonReader that provides the BSON
   * @param decoderContext not used
   * @return the object created from the BSON stream.
   */
  @Override
  @SuppressWarnings("unchecked")
  public C decode(BsonReader bsonReader, DecoderContext decoderContext) {
    return (C) schema.mapToItem(BsonToEntityConverter.read(bsonReader));
  }

  /**
   * This serializes an object into a BSON stream. The serialization is delegated to
   * {@link EntityToBsonConverter}
   * @param bsonWriter that encodes each attribute to BSON
   * @param obj the object to encode
   * @param encoderContext not used
   */
  @Override
  @SuppressWarnings("unchecked")
  public void encode(BsonWriter bsonWriter, C obj, EncoderContext encoderContext) {
    EntityToBsonConverter.write(bsonWriter, schema.itemToMap((S)obj, true));
  }

  /**
   * A getter of the class being encoded.
   * @return the class being encoded
   */
  @Override
  public Class<C> getEncoderClass() {
    return clazz;
  }
}
