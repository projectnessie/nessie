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
package org.projectnessie.api.params;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class DiffParamsTest {

  @Test
  public void testBuilder() {
    DiffParams params =
        DiffParams.builder()
            .fromRef("from")
            .fromHashOnRef("fromHash")
            .toRef("to")
            .toHashOnRef("toHash")
            .build();
    assertThat(params)
        .extracting(
            DiffParams::getFromRef,
            DiffParams::getFromHashOnRef,
            DiffParams::getToRef,
            DiffParams::getToHashOnRef)
        .containsExactly("from", "fromHash", "to", "toHash");
    params = DiffParams.builder().fromRef("from").toRef("to").toHashOnRef("toHash").build();
    assertThat(params)
        .extracting(
            DiffParams::getFromRef,
            DiffParams::getFromHashOnRef,
            DiffParams::getToRef,
            DiffParams::getToHashOnRef)
        .containsExactly("from", null, "to", "toHash");
    params = DiffParams.builder().fromRef("from").toRef("to").build();
    assertThat(params)
        .extracting(
            DiffParams::getFromRef,
            DiffParams::getFromHashOnRef,
            DiffParams::getToRef,
            DiffParams::getToHashOnRef)
        .containsExactly("from", null, "to", null);
  }

  @Test
  public void testValidation() {
    assertThatThrownBy(() -> DiffParams.builder().fromRef("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot build DiffParams, some of required attributes are not set [toRef]");

    assertThatThrownBy(() -> DiffParams.builder().toRef("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot build DiffParams, some of required attributes are not set [fromRef]");
  }
}
