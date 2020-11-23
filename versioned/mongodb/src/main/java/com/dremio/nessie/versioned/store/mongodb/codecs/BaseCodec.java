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

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.SimpleSchema;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.util.Map;

/**
 * Base codec for codecs responsible for the encoding and decoding of Entities to a BSON objects.
 */
public abstract class BaseCodec<T> implements Codec<T> {
  private final Class<T> clazz;
  private final SimpleSchema<T> schema;

  /**
   * Constructor.
   * @param clazz the class type to encode/decode.
   * @param schema the schema of the class.
   */
  protected BaseCodec(Class<T> clazz, SimpleSchema<T> schema) {
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
  public T decode(BsonReader bsonReader, DecoderContext decoderContext) {
    bsonReader.readStartDocument();
    //TODO complete this method.
    bsonReader.readEndDocument();

    return null;
  }

  /**
   * This serializes an object into a BSON stream. The serialization is delegated to
   * {@link EntityToBsonConverter}
   * @param bsonWriter that encodes each attribute to BSON
   * @param obj the object to encode
   * @param encoderContext not used
   */
  @Override
  public void encode(BsonWriter bsonWriter, T obj, EncoderContext encoderContext) {
    final Map<String, Entity> attributes = schema.itemToMap(obj, true);
    bsonWriter.writeStartDocument();
    attributes.forEach((k, v) -> EntityToBsonConverter.writeField(bsonWriter, k, v));
    bsonWriter.writeEndDocument();
  }

  /**
   * A getter of the class being encoded.
   * @return the class being encoded
   */
  @Override
  public Class<T> getEncoderClass() {
    return clazz;
  }
}
