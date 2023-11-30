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

import static org.projectnessie.versioned.storage.common.objtypes.ObjIdElementSerializer.OBJ_ID_SERIALIZER;
import static org.projectnessie.versioned.storage.common.persist.ObjId.deserializeObjId;
import static org.projectnessie.versioned.storage.common.util.Ser.varIntLen;
import static org.projectnessie.versioned.storage.common.util.Util.generateObjIdBuffer;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestObjIdElementSerializer {

  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void serializeRoundTrip(boolean directBuffer) {
    IntFunction<ByteBuffer> alloc =
        len -> directBuffer ? ByteBuffer.allocateDirect(len) : ByteBuffer.allocate(len);

    for (int objIdLen = 0; objIdLen < 256; objIdLen++) {
      ByteBuffer buf = generateObjIdBuffer(directBuffer, objIdLen);

      ObjId id = deserializeObjId(buf.duplicate());

      soft.assertThat(OBJ_ID_SERIALIZER.serializedSize(id))
          .describedAs("len = " + objIdLen)
          .isEqualTo(objIdLen + varIntLen(objIdLen));

      ByteBuffer target = alloc.apply(objIdLen + varIntLen(objIdLen));
      target = OBJ_ID_SERIALIZER.serialize(id, target).flip();
      soft.assertThat(target).describedAs("len = " + objIdLen).isEqualTo(buf);
      soft.assertThat(OBJ_ID_SERIALIZER.deserialize(target.duplicate()))
          .describedAs("len = " + objIdLen)
          .isEqualTo(id);
    }
  }
}
