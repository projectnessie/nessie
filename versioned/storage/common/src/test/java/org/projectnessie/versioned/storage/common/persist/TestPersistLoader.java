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
package org.projectnessie.versioned.storage.common.persist;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPersistLoader {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void findNonExistent() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> PersistLoader.findFactoryByName("this one does not exist"))
        .withMessage("No BackendFactory matched the given filter");
  }

  @Test
  public void findNonMatching() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> PersistLoader.findFactory(f -> false))
        .withMessage("No BackendFactory matched the given filter");
  }

  @Test
  public void findAny() {
    soft.assertThat(PersistLoader.findAny()).isNotNull();
  }

  @Test
  public void findFactoryByName() {
    soft.assertThat(PersistLoader.findFactoryByName(InmemoryBackendFactory.NAME)).isNotNull();
  }
}
