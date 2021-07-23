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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.util.JsonGeneratorDelegate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;

/**
 * Tests that unknown fields in a JSON do not break deserialization.
 *
 * <p>The implementation approach is to construct some valid objects, serialize those to JSON,
 * inject additional (=unknown) fields during serialization and deserializing the "enriched" JSON.
 */
class TestIgnoreProperties {

  static ObjectMapper mapper;

  @BeforeAll
  static void setup() {
    mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
  }

  @Test
  void multiGet() throws Exception {
    doTestUnknown(
        MultiGetContentsRequest.of(ContentsKey.of("foo", "bar")), MultiGetContentsRequest.class);
    doTestUnknown(
        MultiGetContentsResponse.of(
            Collections.singletonList(
                ContentsWithKey.of(ContentsKey.of("foo", "bar"), IcebergTable.of("here/there")))),
        MultiGetContentsResponse.class);
  }

  @Test
  void paginated() throws Exception {
    doTestUnknown(
        EntriesResponse.builder()
            .addEntries(
                Entry.builder().name(ContentsKey.of("entry-name")).type(Type.ICEBERG_TABLE).build())
            .build(),
        EntriesResponse.class);
    doTestUnknown(
        ImmutableLogResponse.builder()
            .addOperations(
                CommitMeta.builder()
                    .commitTime(Instant.now())
                    .authorTime(Instant.now())
                    .properties(Collections.emptyMap())
                    .committer("committer")
                    .author("author")
                    .message("something important")
                    .signedOffBy("signed-by-human")
                    .hash("1234123412341234123412341234123412341234")
                    .build())
            .hasMore(true)
            .token("token")
            .build(),
        LogResponse.class);
  }

  @Test
  void reference() throws Exception {
    doTestUnknown(Branch.of("name", "1234123412341234123412341234123412341234"), Reference.class);
    doTestUnknown(Tag.of("name", "1234123412341234123412341234123412341234"), Reference.class);
    doTestUnknown(Hash.of("1234123412341234123412341234123412341234"), Reference.class);
  }

  @Test
  void operations() throws Exception {
    Operations ops =
        ImmutableOperations.builder()
            .commitMeta(
                CommitMeta.builder()
                    .message("some message")
                    .commitTime(Instant.now())
                    .authorTime(Instant.now())
                    .hash("1234123412341234123412341234123412341234")
                    .build())
            .addOperations(Delete.of(ContentsKey.of("delete", "something")))
            .addOperations(Unchanged.of(ContentsKey.of("nothing", "changed")))
            .addOperations(
                Put.of(ContentsKey.of("put", "iceberg"), IcebergTable.of("here/and/there")))
            .addOperations(
                Put.of(
                    ContentsKey.of("put", "delta"),
                    ImmutableDeltaLakeTable.builder()
                        .lastCheckpoint("checkpoint")
                        .addMetadataLocationHistory("meta", "data", "location")
                        .addCheckpointLocationHistory("checkpoint")
                        .id("delta-id")
                        .build()))
            .addOperations(
                Put.of(
                    ContentsKey.of("put", "hive"),
                    ImmutableHiveTable.builder()
                        .addPartitions(new byte[5])
                        .tableDefinition(new byte[10])
                        .id("hive-id")
                        .build()))
            .build();
    doTestUnknown(ops, Operations.class);
  }

  private <T> void doTestUnknown(T value, Class<T> type) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    ObjectWriter objWriter = mapper.writerFor(type);

    JsonGenerator jsonGen = objWriter.createGenerator(buf, JsonEncoding.UTF8);

    AtomicInteger counter = new AtomicInteger();

    JsonGenerator mock =
        new JsonGeneratorDelegate(jsonGen) {
          @Override
          public void writeFieldName(String name) throws IOException {
            unknownFields();
            super.writeFieldName(name);
          }

          @Override
          public void writeFieldName(SerializableString name) throws IOException {
            unknownFields();
            super.writeFieldName(name);
          }

          private void unknownFields() throws IOException {
            jsonGen.writeStringField("someUnknownField_" + counter.incrementAndGet(), "foo bar");

            jsonGen.writeArrayFieldStart("someUnknownField_" + counter.incrementAndGet());
            jsonGen.writeString("meep");
            jsonGen.writeString("meep");
            jsonGen.writeString("meep");
            jsonGen.writeEndArray();

            jsonGen.writeObjectFieldStart("someUnknownField_" + counter.incrementAndGet());
            jsonGen.writeStringField("someUnknownField_" + counter.incrementAndGet(), "foo bar");
            jsonGen.writeEndObject();
          }
        };

    objWriter.writeValue(mock, value);

    String str = buf.toString("UTF-8");

    // System.err.println(str);

    ObjectReader reader = mapper.readerFor(type);
    Object deserialized = reader.readValue(str);

    Assertions.assertEquals(value, deserialized);
  }
}
