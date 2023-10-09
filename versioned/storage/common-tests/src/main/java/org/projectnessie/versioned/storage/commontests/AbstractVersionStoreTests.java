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
package org.projectnessie.versioned.storage.commontests;

import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.tests.AbstractMergeScenarios;
import org.projectnessie.versioned.tests.AbstractRepositoryConfig;
import org.projectnessie.versioned.tests.AbstractVersionStoreTestBase;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class AbstractVersionStoreTests extends AbstractVersionStoreTestBase {

  @NessiePersist protected static Persist persist;

  @Override
  protected VersionStore store() {
    return new VersionStoreImpl(persist);
  }

  @Nested
  public class MergeScenarios extends AbstractMergeScenarios {
    public MergeScenarios() {
      super(AbstractVersionStoreTests.this.store());
    }
  }

  @Nested
  public class RepositoryConfig extends AbstractRepositoryConfig {
    public RepositoryConfig() {
      super(AbstractVersionStoreTests.this.store());
    }
  }

  @Nested
  public class CommitConsistency extends AbstractCommitConsistency {
    public CommitConsistency() {
      super(AbstractVersionStoreTests.this.store());
    }
  }
}
