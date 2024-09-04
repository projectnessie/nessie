/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.common.objtypes;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.contextualReader;
import static org.projectnessie.versioned.storage.common.objtypes.GenericObj.VERSION_TOKEN_ATTRIBUTE;
import static org.projectnessie.versioned.storage.common.objtypes.GenericObjTypeMapper.newGenericObjType;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.commontests.objtypes.SimpleTestObj;
import org.projectnessie.versioned.storage.commontests.objtypes.VersionedTestObj;

@ExtendWith(SoftAssertionsExtension.class)
public class TestGenericObj {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void genericObj(ObjType realType, ObjId id, Obj realObj) throws Exception {
    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    ObjType genericType = newGenericObjType("genericType_" + UUID.randomUUID());
    String versionToken =
        (realObj instanceof UpdateableObj) ? ((UpdateableObj) realObj).versionToken() : null;

    String json = mapper.writeValueAsString(realObj);

    Obj genericObj =
        contextualReader(mapper, genericType, id, versionToken, realObj.referenced())
            .readValue(json, genericType.targetClass());
    soft.assertThat(genericObj)
        .isInstanceOf(GenericObj.class)
        .extracting(GenericObj.class::cast)
        .extracting(GenericObj::id, GenericObj::type)
        .containsExactly(realObj.id(), genericType);

    if (realObj instanceof UpdateableObj) {
      soft.assertThat(((GenericObj) genericObj).attributes())
          .containsEntry(VERSION_TOKEN_ATTRIBUTE, versionToken);
    }

    String jsonGeneric = mapper.writeValueAsString(genericObj);
    Obj deserRealObj =
        contextualReader(mapper, realType, id, versionToken, realObj.referenced())
            .readValue(jsonGeneric, realType.targetClass());
    soft.assertThat(deserRealObj).isEqualTo(realObj);
  }

  static Stream<Arguments> genericObj() {
    // We don't persist anything, so we can reuse this ID.
    ObjId id = randomObjId();

    return Stream.of(
        arguments(
            SimpleTestObj.TYPE,
            id,
            SimpleTestObj.builder()
                .id(id)
                .addList("one", "two", "three")
                .putMap("a", "A")
                .putMap("b", "B")
                .text("some text")
                .build()),
        //
        arguments(
            VersionedTestObj.TYPE,
            id,
            VersionedTestObj.builder()
                .id(id)
                .someValue("some value")
                .versionToken("my version token")
                .build()));
  }
}
