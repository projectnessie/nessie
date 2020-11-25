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

import java.util.Map;

import org.bson.BsonReader;
import org.bson.json.JsonReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

public class TestBsonToEntityConverter {
  @Test
  public void readNoDocumentRoot() {
    final BsonReader reader = new JsonReader("1");
    Assertions.assertThrows(UnsupportedOperationException.class, () -> BsonToEntityConverter.read(reader));
  }

  @Test
  public void readUnsupportedType() {
    final BsonReader reader = new JsonReader("{\"$numberDouble\": \"1\"}");
    reader.readBsonType();
    Assertions.assertThrows(UnsupportedOperationException.class, () -> BsonToEntityConverter.read(reader));
  }

  @Test
  public void readBoolean() {
    final BsonReader reader = new JsonReader("{\"value\": true}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(Entity.ofBoolean(true), entities.get("value"));
  }

  @Test
  public void readNumber() {
    final BsonReader reader = new JsonReader("{\"value\": \"n55\"}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(Entity.ofNumber(55), entities.get("value"));
  }

  @Test
  public void readString() {
    final BsonReader reader = new JsonReader("{\"value\": \"sString\"}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(Entity.ofString("String"), entities.get("value"));
  }

  @Test
  public void readBinary() {
    final BsonReader reader = new JsonReader("{\"value\": {\"$binary\": {\"base64\": \"AQI=\", \"subType\": \"00\"}}}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(Entity.ofBinary(new byte[]{1, 2}), entities.get("value"));
  }

  @Test
  public void readMap() {
    final BsonReader reader = new JsonReader("{\"value\": {}}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(Entity.ofMap(ImmutableMap.of()), entities.get("value"));
  }

  @Test
  public void readList() {
    final BsonReader reader = new JsonReader("{\"value\": [true, \"n5\", \"sStr\", false]}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(
        Entity.ofList(Entity.ofNumber("5"), Entity.ofString("Str"), Entity.ofBoolean(false)), entities.get("value"));
  }

  @Test
  public void readListUnsupportedType() {
    final BsonReader reader = new JsonReader("{\"value\": [true, {\"$numberDouble\": \"1\"}]}");
    reader.readBsonType();
    Assertions.assertThrows(UnsupportedOperationException.class, () -> BsonToEntityConverter.read(reader));
  }

  @Test
  public void readStringSet() {
    final BsonReader reader = new JsonReader("{\"value\": [false, \"string1\"]}");
    reader.readBsonType();
    final Map<String, Entity> entities = BsonToEntityConverter.read(reader);
    Assertions.assertEquals(Entity.ofStringSet("string1"), entities.get("value"));
  }

  @Test
  public void readStringSetWithoutString() {
    final BsonReader reader = new JsonReader("{\"value\": [false, 5]}");
    reader.readBsonType();
    Assertions.assertThrows(UnsupportedOperationException.class, () -> BsonToEntityConverter.read(reader));
  }
}
