/*
 * Copyright (C) 2022 Dremio
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

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.deserializeObjId;
import static org.projectnessie.versioned.storage.common.util.Ser.varIntLen;
import static org.projectnessie.versioned.storage.common.util.Util.generateObjIdBuffer;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCommitOp {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> serializeRoundTrip() {
    return Stream.of(
        arguments(false, 1, false),
        arguments(false, 1, true),
        arguments(true, 1, false),
        arguments(true, 1, true),
        arguments(false, 10, false),
        arguments(false, 10, true),
        arguments(true, 10, false),
        arguments(true, 10, true),
        arguments(false, 100, false),
        arguments(false, 100, true),
        arguments(true, 100, false),
        arguments(true, 100, true),
        arguments(false, 256, false),
        arguments(false, 256, true),
        arguments(true, 256, false),
        arguments(true, 256, true));
  }

  @ParameterizedTest
  @MethodSource("serializeRoundTrip")
  public void serializeRoundTrip(boolean directBuffer, int objIdSize, boolean withContentId) {
    IntFunction<ByteBuffer> alloc =
        len -> directBuffer ? ByteBuffer.allocateDirect(len) : ByteBuffer.allocate(len);

    int len = objIdSize + varIntLen(objIdSize);

    ByteBuffer idBuffer = generateObjIdBuffer(directBuffer, objIdSize);

    UUID contentId = withContentId ? randomUUID() : null;
    CommitOp entry = commitOp(Action.ADD, 1, deserializeObjId(idBuffer.duplicate()), contentId);

    int overhead = 1 + 1 + 16;

    soft.assertThat(COMMIT_OP_SERIALIZER.serializedSize(entry))
        .describedAs("len = " + len)
        .isEqualTo(len + overhead);

    ByteBuffer target = alloc.apply(len + overhead);
    target = COMMIT_OP_SERIALIZER.serialize(entry, target).flip();
    ByteBuffer targetCmp =
        alloc
            .apply(len + overhead)
            .put((byte) Action.ADD.value())
            .put((byte) 1)
            .put(idBuffer.duplicate());
    if (withContentId) {
      targetCmp
          .putLong(contentId.getMostSignificantBits())
          .putLong(contentId.getLeastSignificantBits());
    } else {
      targetCmp.putLong(0L).putLong(0L);
    }
    targetCmp.flip();
    soft.assertThat(target).describedAs("len = " + len).isEqualTo(targetCmp);
    soft.assertThat(COMMIT_OP_SERIALIZER.deserialize(target.duplicate()))
        .describedAs("len = " + len)
        .isEqualTo(entry);
  }
}
