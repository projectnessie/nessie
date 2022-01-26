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
package org.projectnessie.versioned.persist.tests;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.tests.AbstractVersionStoreTestBase;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterConfigItem(name = "max.key.list.size", value = "2048")
public abstract class AbstractDatabaseAdapterVersionStoreTest extends AbstractVersionStoreTestBase {

  @NessieDbAdapter static VersionStore<String, String, StringStoreWorker.TestEnum> store;

  @Override
  protected VersionStore<String, String, StringStoreWorker.TestEnum> store() {
    return store;
  }

  @Nested
  public class SingleBranch extends AbstractSingleBranch {
    public SingleBranch() {
      super(AbstractDatabaseAdapterVersionStoreTest.store);
    }
  }

  @Nested
  public class DuplicateTable extends AbstractDuplicateTable {
    public DuplicateTable() {
      super(AbstractDatabaseAdapterVersionStoreTest.store);
    }
  }
}
