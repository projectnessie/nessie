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

import java.io.StringWriter;
import java.util.Map;

import org.bson.json.JsonWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

class TestEntityToBsonConverter {
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
    read(ImmutableMap.of("value", Entity.ofBinary(new byte[]{1, 2})),
        "{\"value\": {\"$binary\": {\"base64\": \"AQI=\", \"subType\": \"00\"}}}");
  }

  @Test
  public void readEmptyMap() {
    read(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of())), "{\"value\": {}}");
  }

  @Test
  public void readEmptyList() {
    read(ImmutableMap.of("value", Entity.ofList()), "{\"value\": []}");
  }

  private void read(Map<String, Entity> attributes, String expected) {
    final StringWriter writer = new StringWriter();
    CodecProvider.ENTITY_TO_BSON_CONVERTER.write(new JsonWriter(writer), attributes);
    Assertions.assertEquals(expected, writer.toString());
  }
}
