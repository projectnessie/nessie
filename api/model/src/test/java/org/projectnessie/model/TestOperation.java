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
package org.projectnessie.model;

import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestOperation {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void emptyKey() {
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> Operation.Put.of(ContentKey.of(), IcebergTable.of("meta", 1, 2, 3, 4)))
        .withMessage("Content key must not be empty");
    soft.assertThatIllegalStateException()
        .isThrownBy(
            () -> Operation.Put.of(ContentKey.of(List.of()), IcebergTable.of("meta", 1, 2, 3, 4)))
        .withMessage("Content key must not be empty");
  }
}
