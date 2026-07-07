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
package org.projectnessie.versioned.storage.common.json;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestObjIdSerialization {

  final ObjectMapper objectMapper = JsonMapper.builder().addModule(new PersistModule()).build();

  static Stream<ObjId> objectIds() {
    return Stream.of(
        ObjId.EMPTY_OBJ_ID,
        // generic
        ObjId.objIdFromString("cafebabe"),
        // SHA-256
        ObjId.objIdFromString("cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe"));
  }

  @ParameterizedTest
  @MethodSource("objectIds")
  void testSerializeDeserialize(ObjId id) {
    byte[] bytes = objectMapper.writeValueAsBytes(id);
    ObjId actual = objectMapper.readValue(bytes, ObjId.class);
    assertThat(actual).isEqualTo(id);
  }
}
