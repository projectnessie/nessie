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
package org.projectnessie.gc.contents.inmem;

import java.util.UUID;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.contents.tests.AbstractPersistenceSpi;

public class TestInMemoryPersistenceSpi extends AbstractPersistenceSpi {

  @Override
  protected PersistenceSpi createPersistenceSpi() {
    return new InMemoryPersistenceSpi();
  }

  @Override
  protected void assertDeleted(UUID id) {
    // noop
  }
}
