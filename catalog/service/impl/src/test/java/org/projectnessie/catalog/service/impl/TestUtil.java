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
package org.projectnessie.catalog.service.impl;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.model.id.NessieId.emptyNessieId;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromBytes;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteArray;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.ObjId.zeroLengthObjId;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestUtil {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void nessieIdObjId(NessieId nessieId, ObjId objId) {
    NessieId fromObjId = Util.objIdToNessieId(objId);
    soft.assertThat(fromObjId).isEqualTo(nessieId);
    soft.assertThat(fromObjId)
        .extracting(
            NessieId::size,
            NessieId::id,
            NessieId::idAsBytes,
            NessieId::idAsString,
            NessieId::toString,
            NessieId::hashCode)
        .containsExactly(
            objId.size(),
            objId.asByteBuffer(),
            objId.asByteArray(),
            objId.toString(),
            objId.toString(),
            objId.hashCode());

    ObjId fromNessieId = Util.nessieIdToObjId(nessieId);
    soft.assertThat(fromNessieId).isEqualTo(objId);
    soft.assertThat(fromNessieId)
        .extracting(
            ObjId::size,
            ObjId::asByteBuffer,
            ObjId::asByteArray,
            ObjId::toString,
            ObjId::toString,
            ObjId::hashCode)
        .containsExactly(
            nessieId.size(),
            nessieId.id(),
            nessieId.idAsBytes(),
            nessieId.idAsString(),
            nessieId.toString(),
            nessieId.hashCode());
  }

  static Stream<Arguments> nessieIdObjId() {
    byte[] bytes1 = {0x01, 0x23, 0x45, 0x67};
    byte[] bytes2 = {0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab};
    byte[] bytes3 = {0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef};
    byte[] bytes4 = {
      //
      0x01,
      0x23,
      0x45,
      0x67,
      (byte) 0x89,
      (byte) 0xab,
      (byte) 0xcd,
      (byte) 0xef,
      //
      0x00,
      0x11,
      0x22,
      0x33,
      0x44,
      0x55,
      0x66,
      0x77,
      //
      (byte) 0x88,
      (byte) 0x99,
      (byte) 0xaa,
      (byte) 0xbb,
      (byte) 0xcc,
      (byte) 0xdd,
      (byte) 0xee,
      (byte) 0xff,
      //
      0x70,
      0x71,
      0x72,
      0x73,
      0x74,
      0x75,
      0x76,
      0x77,
    };

    return Stream.concat(
        Stream.of(
            arguments(nessieIdFromBytes(bytes1), objIdFromByteArray(bytes1)),
            arguments(nessieIdFromBytes(bytes2), objIdFromByteArray(bytes2)),
            arguments(nessieIdFromBytes(bytes3), objIdFromByteArray(bytes3)),
            arguments(nessieIdFromBytes(bytes4), objIdFromByteArray(bytes4)),
            //
            arguments(nessieIdFromBytes(new byte[0]), objIdFromByteArray(new byte[0])),
            arguments(emptyNessieId(), zeroLengthObjId()),
            arguments(nessieIdFromBytes(new byte[0]), zeroLengthObjId()),
            arguments(emptyNessieId(), objIdFromByteArray(new byte[0]))),
        IntStream.range(0, 20)
            .mapToObj(x -> randomObjId())
            .map(objId -> arguments(nessieIdFromBytes(objId.asByteArray()), objId)));
  }
}
