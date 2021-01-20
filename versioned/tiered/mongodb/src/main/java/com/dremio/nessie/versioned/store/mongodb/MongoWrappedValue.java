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

import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.types.Binary;

import com.dremio.nessie.tiered.builder.BaseWrappedValue;
import com.google.protobuf.ByteString;

class MongoWrappedValue<C extends BaseWrappedValue<C>> extends MongoBaseValue<C> implements BaseWrappedValue<C> {

  static final String VALUE = "value";

  static <C extends BaseWrappedValue<C>> void produce(Document document, C v) {
    produceBase(document, v)
        .value(ByteString.copyFrom(((Binary) document.get(VALUE)).getData()));
  }

  MongoWrappedValue(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @SuppressWarnings("unchecked")
  @Override
  public C value(ByteString value) {
    addProperty(VALUE);
    bsonWriter.writeBinaryData(VALUE, MongoSerDe.serializeBytes(value));
    return (C) this;
  }

  @Override
  BsonWriter build() {
    checkPresent(VALUE, "value");
    return super.build();
  }
}
