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

import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonSerializationException;
import org.bson.internal.Base64;
import org.bson.json.JsonReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

class TestBsonToEntityConverter {
  private static final long SEED = -9082734792382L;

  @Test
  public void readInvalidState() {
    // Don't read the BSON type so the state is invalid.
    readSerializationError(new JsonReader("1"));
  }

  @Test
  public void readNoDocumentRoot() {
    readSerializationError(getReader("1"));
  }

  @Test
  public void readUnsupportedType() {
    readUnsupportedError(getReader("{\"key\": {\"$numberDouble\": \"1\"}}"));
  }

  @Test
  public void readUnsupportedTypeInList() {
    readUnsupportedError(getReader("{\"key\": [{\"$date\": {\"$numberLong\": \"10002348235\"}}]}"));
  }

  @Test
  public void readUnsupportedTypeInMap() {
    readUnsupportedError(getReader("{\"key\": {\"mapKey\": {\"$numberDecimal\": \"1.3453\"}}}"));
  }

  @Test
  public void readEmptyDocument() {
    final BsonReader reader = getReader("{}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertTrue(entities.isEmpty());
  }

  @Test
  public void readBoolean() {
    final BsonReader reader = getReader("{\"value\": true}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofBoolean(true), entities.get("value"));
  }

  @Test
  public void readNumberInt32() {
    final BsonReader reader = getReader("{\"value\": 55}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofNumber(55), entities.get("value"));
  }

  @Test
  public void readNumberInt64() {
    final BsonReader reader = getReader("{\"value\": 50817234087123405}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofNumber(50817234087123405L), entities.get("value"));
  }

  @Test
  public void readString() {
    final BsonReader reader = getReader("{\"value\": \"String\"}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofString("String"), entities.get("value"));
  }

  @Test
  public void readBinary() {
    final byte[] buffer = SampleEntities.createBinary(SEED, 10);
    final BsonReader reader = getReader(String.format(
        "{\"value\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}", Base64.encode(buffer)));
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofBinary(buffer), entities.get("value"));
  }

  @Test
  public void readEmptyMap() {
    final BsonReader reader = getReader("{\"value\": {}}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofMap(ImmutableMap.of()), entities.get("value"));
  }

  @Test
  public void readMap() {
    final BsonReader reader = getReader(
        "{\"value\": {\"key1\": \"str1\", \"key2\": false, \"key3\": []}}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofMap(ImmutableMap.of(
        "key1", Entity.ofString("str1"),
        "key2", Entity.ofBoolean(false),
        "key3", Entity.ofList()
    )), entities.get("value"));
  }

  @Test
  public void readEmptyList() {
    final BsonReader reader = getReader("{\"value\": []}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(Entity.ofList(), entities.get("value"));
  }

  @Test
  public void readList() {
    final BsonReader reader = getReader("{\"value\": [5, \"Str\", false, {}]}");
    final Map<String, Entity> entities = CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader);
    Assertions.assertEquals(
        Entity.ofList(Entity.ofNumber(5), Entity.ofString("Str"), Entity.ofBoolean(false), Entity.ofMap(ImmutableMap.of())),
        entities.get("value"));
  }

  @Test
  public void readListUnsupportedType() {
    final BsonReader reader = getReader("{\"value\": [{\"$numberDouble\": \"1\"}]}");
    readUnsupportedError(reader);
  }

  private BsonReader getReader(String bson) {
    final BsonReader reader = new JsonReader(bson);
    reader.readBsonType();
    return reader;
  }

  private void readUnsupportedError(BsonReader reader) {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader));
  }

  private void readSerializationError(BsonReader reader) {
    Assertions.assertThrows(
        BsonSerializationException.class,
        () -> CodecProvider.BSON_TO_ENTITY_CONVERTER.read(reader));
  }
}
