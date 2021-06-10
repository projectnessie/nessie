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
package org.projectnessie.versioned.jgit;

import java.io.IOException;
import org.eclipse.jgit.lib.Repository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

public abstract class AbstractITJGitVersionStore extends AbstractITVersionStore {
  protected Repository repository;
  protected VersionStore<String, String, StringSerializer.TestEnum> store;

  protected static final StoreWorker<String, String, StringSerializer.TestEnum> WORKER =
      StoreWorker.of(StringSerializer.getInstance(), StringSerializer.getInstance());

  abstract void setUp() throws IOException;

  @Disabled("NYI")
  protected void checkDiff() throws VersionStoreException {
    super.checkDiff();
  }

  @AfterEach
  void tearDown() {
    repository.close();
  }

  @Override
  protected VersionStore<String, String, StringSerializer.TestEnum> store() {
    return store;
  }
}
