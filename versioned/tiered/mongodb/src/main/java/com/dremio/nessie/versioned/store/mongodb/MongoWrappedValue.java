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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.bson.BsonReader;
import org.bson.BsonWriter;

import com.dremio.nessie.tiered.builder.BaseWrappedValue;
import com.google.protobuf.ByteString;

class MongoWrappedValue<C extends BaseWrappedValue<C>> extends MongoBaseValue<C> implements BaseWrappedValue<C> {

  static final String VALUE = "value";

  @SuppressWarnings("rawtypes")
  static final Map<String, BiConsumer<BaseWrappedValue, BsonReader>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(VALUE, (c, r) -> c.value(MongoSerDe.deserializeBytes(r)));
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
