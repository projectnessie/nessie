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
package org.projectnessie.model;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCommitMetaSerde {

  @Test
  void testInstantSerde() throws IOException {
    Instant expectedTime = Instant.ofEpochMilli(423902715000L);

    // set up serialization context
    Writer jsonWriter = new StringWriter();
    ObjectMapper mapper = new ObjectMapper();
    JsonGenerator jsonGenerator = new JsonFactory().createGenerator(jsonWriter);
    SerializerProvider serializerProvider = mapper.getSerializerProvider();

    // set up serde
    CommitMeta.InstantSerializer ser = new CommitMeta.InstantSerializer();
    CommitMeta.InstantDeserializer de = new CommitMeta.InstantDeserializer();

    // serialize
    ser.serialize(expectedTime, jsonGenerator, serializerProvider);
    jsonGenerator.flush();

    // check serialization
    String result = jsonWriter.toString();
    Assertions.assertEquals("\"1983-06-08T06:45:15Z\"", result);

    // set up deserialization context
    InputStream stream = new ByteArrayInputStream(result.getBytes(StandardCharsets.UTF_8));
    JsonParser parser = mapper.getFactory().createParser(stream);
    DeserializationContext ctxt = mapper.getDeserializationContext();
    parser.nextToken();

    // check deserialization
    Instant actualTime = de.deserialize(parser, ctxt);
    Assertions.assertEquals(expectedTime, actualTime);
  }
}
