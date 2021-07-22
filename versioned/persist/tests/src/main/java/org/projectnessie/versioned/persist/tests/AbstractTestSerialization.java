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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.spi.DbObjectsSerializers;

public class AbstractTestSerialization {

  public static class SerializationParam {
    public final String name;
    public ObjectMapper mapper;
    public FormatSchema schema;
    public ObjectWriter writer;
    public ObjectReader reader;
    public Supplier<Object> generator;
    public Class<?> type;

    SerializationParam(String name, ObjectMapper mapper) {
      this.name = name;

      mapper = mapper.findAndRegisterModules();
      mapper = DbObjectsSerializers.register(mapper);

      this.mapper = mapper;
      this.writer = mapper.writer();
      this.reader = mapper.reader();
    }

    private SerializationParam(
        String name,
        ObjectMapper mapper,
        FormatSchema schema,
        ObjectWriter writer,
        ObjectReader reader,
        Supplier<Object> generator,
        Class<?> type) {
      this.name = name;
      this.mapper = mapper;
      this.schema = schema;
      this.writer = writer;
      this.reader = reader;
      this.generator = generator;
      this.type = type;
    }

    public SerializationParam withType(Supplier<Object> generator, Class<?> type) {
      return new SerializationParam(name, mapper, schema, writer, reader, generator, type);
    }

    @Override
    public String toString() {
      return name + " " + type.getSimpleName();
    }
  }

  public static Stream<SerializationParam> paramsUntyped() {
    return Stream.of(
        // new SerializationParam("smile", DbObjectsSerializers.register(new SmileMapper())),
        new SerializationParam("ion", new IonObjectMapper()),
        new SerializationParam("cbor", new CBORMapper()));
  }

  public static Supplier<Stream<SerializationParam>> params;

  public static Stream<SerializationParam> params() {
    return params.get();
  }

  @ParameterizedTest
  @MethodSource("params")
  @Disabled("exists to compare serialized sizes of different jackson-dataformat implementations")
  public void entries(SerializationParam ser) throws Exception {
    Object value = ser.generator.get();
    byte[] serialized = ser.writer.writeValueAsBytes(value);
    System.err.printf("%s serialized size: %s%n", ser, serialized.length);

    Object deserialized = ser.reader.readValue(serialized, ser.type);
    assertThat(deserialized).isEqualTo(value);
  }

  @ParameterizedTest
  @MethodSource("params")
  public void serialize1k(SerializationParam ser) throws Exception {
    Object value = ser.generator.get();
    for (int i = 0; i < 1_000; i++) {
      ser.writer.writeValueAsBytes(value);
    }
  }

  @ParameterizedTest
  @MethodSource("params")
  public void deserialize1k(SerializationParam ser) throws Exception {
    Object value = ser.generator.get();
    byte[] serialized = ser.writer.writeValueAsBytes(value);

    for (int i = 0; i < 1_000; i++) {
      ser.reader.readValue(serialized, ser.type);
    }
  }

  public static ContentsId randomId() {
    return ContentsId.of(UUID.randomUUID().toString());
  }

  public static Key randomKey() {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    return Key.of("some" + r.nextInt(10000), "other" + r.nextInt(10000));
  }

  public static Hash randomHash() {
    return Hash.of(randomBytes(16));
  }

  public static ByteString randomBytes(int num) {
    byte[] raw = new byte[num];
    ThreadLocalRandom.current().nextBytes(raw);
    return UnsafeByteOperations.unsafeWrap(raw);
  }

  public static NamedRef randomRef() {
    return BranchName.of("reference" + ThreadLocalRandom.current().nextLong());
  }
}
