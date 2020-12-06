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

import java.io.StringWriter;
import java.util.Map;

import org.bson.internal.Base64;
import org.bson.json.JsonWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

class TestEntityToBsonConverter {
  private static final long SEED = 16234910234034L;

  @Test
  public void readEmptyAttributes() {
    read(ImmutableMap.of(), "{}");
  }

  @Test
  public void readBoolean() {
    read(ImmutableMap.of("value", Entity.ofBoolean(true)), "{\"value\": true}");
  }

  @Test
  public void readNumber() {
    read(ImmutableMap.of("value", Entity.ofNumber(5)), "{\"value\": 5}");
  }

  @Test
  public void readString() {
    read(ImmutableMap.of("value", Entity.ofString("string")), "{\"value\": \"string\"}");
  }

  @Test
  public void readBinary() {
    byte[] buffer = SampleEntities.createBinary(SEED, 10);
    read(ImmutableMap.of("value", Entity.ofBinary(buffer)),
        String.format("{\"value\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"00\"}}}", Base64.encode(buffer)));
  }

  @Test
  public void readEmptyMap() {
    read(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of())), "{\"value\": {}}");
  }

  @Test
  public void readMap() {
    read(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of(
        "key", Entity.ofNumber(5),
        "key2", Entity.ofBoolean(false),
        "key3", Entity.ofMap(ImmutableMap.of())
      ))), "{\"value\": {\"key\": 5, \"key2\": false, \"key3\": {}}}");
  }

  @Test
  public void readEmptyList() {
    read(ImmutableMap.of("value", Entity.ofList()), "{\"value\": []}");
  }

  @Test
  public void readList() {
    read(ImmutableMap.of("value",
        Entity.ofList(Entity.ofString("myValue"), Entity.ofNumber(999), Entity.ofMap(ImmutableMap.of()))),
        "{\"value\": [\"myValue\", 999, {}]}");
  }

  private void read(Map<String, Entity> attributes, String expected) {
    final StringWriter writer = new StringWriter();
    CodecProvider.ENTITY_TO_BSON_CONVERTER.write(new JsonWriter(writer), attributes);
    Assertions.assertEquals(expected, writer.toString());
  }
}
