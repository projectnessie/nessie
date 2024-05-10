/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.model.schema;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.types.NessieType;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieSchema {
  @InjectSoftAssertions protected SoftAssertions soft;

  static ObjectMapper mapper;

  @BeforeAll
  public static void createMapper() {
    mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).findAndRegisterModules();
  }

  @Test
  public void allTypes() throws Exception {
    NessieStruct.Builder struct = NessieStruct.builder();

    Function<NessieField.Builder, Stream<NessieField>> nullNotNull =
        builder -> {
          NessieField notNull = builder.nullable(true).build();
          return Stream.of(
              notNull,
              NessieField.builder()
                  .from(notNull)
                  .name(notNull.name() + " not null")
                  .doc(notNull.doc() + " not null")
                  .nullable(false)
                  .build());
        };

    AtomicInteger columnId = new AtomicInteger();
    Stream.concat(
            NessieType.primitiveTypes().stream(),
            Stream.of(
                NessieType.fixedType(666),
                NessieType.timeType(3, false),
                NessieType.timeType(3, true),
                NessieType.timeType(9, false),
                NessieType.timeType(9, true),
                NessieType.timestampType(3, false),
                NessieType.timestampType(3, true),
                NessieType.timestampType(9, false),
                NessieType.timestampType(9, true)))
        .flatMap(
            primitiveType ->
                nullNotNull.apply(
                    NessieField.builder()
                        .id(UUID.randomUUID())
                        .name(primitiveType.asString())
                        .type(primitiveType)
                        .doc("doc for " + primitiveType.asString())
                        .icebergId(columnId.incrementAndGet())))
        .forEach(struct::addField);

    NessieId schemaId = NessieId.randomNessieId();
    NessieSchema schema = NessieSchema.nessieSchema(schemaId, struct.build(), 42, emptyList());

    String json = mapper.writeValueAsString(schema);
    NessieSchema deserialized = mapper.readValue(json, NessieSchema.class);

    soft.assertThat(deserialized.struct().fields())
        .containsExactlyElementsOf(schema.struct().fields());
    soft.assertThat(deserialized).isEqualTo(schema);
    soft.assertThat(mapper.writeValueAsString(deserialized)).isEqualTo(json);
  }

  @ParameterizedTest
  @MethodSource
  public void jsonSerialization(String json, Object object, Class<?> type) throws Exception {
    String ser = mapper.writeValueAsString(object);
    Object reser = mapper.readValue(ser, type);
    soft.assertThat(reser).isEqualTo(object);

    Object deser = mapper.readValue(json, type);
    soft.assertThat(deser).isEqualTo(object);
  }

  static Stream<Arguments> jsonSerialization() {
    UUID id = UUID.fromString("8c4f7079-5905-4604-9333-e928d21613df");

    return Stream.of(
        arguments(
            "{\n"
                + "  \"sourceFieldId\" : \"8c4f7079-5905-4604-9333-e928d21613df\",\n"
                + "  \"type\" : {\n"
                + "    \"type\" : \"int\"\n"
                + "  },\n"
                + "  \"transformSpec\" : \"bucket[42]\",\n"
                + "  \"nullOrder\" : \"nulls-first\",\n"
                + "  \"direction\" : \"asc\"\n"
                + "}",
            NessieSortField.builder()
                .direction(NessieSortDirection.ASC)
                .nullOrder(NessieNullOrder.NULLS_FIRST)
                .type(NessieType.intType())
                .transformSpec(NessieFieldTransform.bucket(42))
                .sourceFieldId(id)
                .build(),
            NessieSortField.class),
        //
        arguments(
            "{\n"
                + "  \"sourceFieldId\" : \"8c4f7079-5905-4604-9333-e928d21613df\",\n"
                + "  \"type\" : {\n"
                + "    \"type\" : \"int\"\n"
                + "  },\n"
                + "  \"transformSpec\" : \"bucket[42]\",\n"
                + "  \"nullOrder\" : \"nulls-last\",\n"
                + "  \"direction\" : \"desc\"\n"
                + "}",
            NessieSortField.builder()
                .direction(NessieSortDirection.DESC)
                .nullOrder(NessieNullOrder.NULLS_LAST)
                .type(NessieType.intType())
                .transformSpec(NessieFieldTransform.bucket(42))
                .sourceFieldId(id)
                .build(),
            NessieSortField.class),
        //
        arguments(
            "{\n"
                + "  \"sourceFieldId\" : \"8c4f7079-5905-4604-9333-e928d21613df\",\n"
                + "  \"type\" : {\n"
                + "    \"type\" : \"int\"\n"
                + "  },\n"
                + "  \"transformSpec\" : \"bucket[42]\",\n"
                + "  \"nullOrder\" : \"nulls-random\",\n"
                + "  \"direction\" : \"mixed\"\n"
                + "}",
            NessieSortField.builder()
                // unknown sort-direction + null-order
                .direction(ImmutableNessieSortDirection.of("mixed", "mixed"))
                .nullOrder(ImmutableNessieNullOrder.of("nulls-random", "nulls-random"))
                .type(NessieType.intType())
                .transformSpec(NessieFieldTransform.bucket(42))
                .sourceFieldId(id)
                .build(),
            NessieSortField.class)
        //
        );
  }
}
