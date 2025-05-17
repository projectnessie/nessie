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
package org.projectnessie.server;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.jaxrs.tests.BaseTestNessieRest;
import org.projectnessie.versioned.storage.common.persist.Persist;

@ExtendWith(QuarkusNessieClientResolver.class)
public abstract class AbstractQuarkusRest extends BaseTestNessieRest {

  @Inject public Instance<Persist> persist;

  @Override
  @AfterEach
  public void tearDown() throws Exception {
    try {
      Persist p = persist.select(Default.Literal.INSTANCE).get();
      if (p != null) {
        try {
          p.erase();
        } finally {
          repositoryLogic(p).initialize("main");
        }
      }
    } finally {
      super.tearDown();
    }
  }
}
