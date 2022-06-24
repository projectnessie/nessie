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
package org.projectnessie.deltalake;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ITDeltaNessieExtensions extends AbstractDeltaTest {

  private static final String BRANCH_NAME = "ITDeltaNessieExtensions";

  public ITDeltaNessieExtensions() {
    super(BRANCH_NAME);
  }

  @Test
  void testCreateTag() throws Exception {
    String mainHash =
        api.getAllReferences().get().getReferences().stream()
            .filter(b -> b.getName().equals(BRANCH_NAME))
            .findFirst()
            .get()
            .getHash();

    assertThat(sql("LIST REFERENCES in spark_catalog"))
        .containsExactlyInAnyOrder(row("Branch", BRANCH_NAME, mainHash));

    assertThat(sql("CREATE TAG %s", "testTag")).containsExactly(row("Tag", "testTag", mainHash));

    assertThat(sql("LIST REFERENCES"))
        .containsExactlyInAnyOrder(
            row("Branch", BRANCH_NAME, mainHash), row("Tag", "testTag", mainHash));
  }
}
