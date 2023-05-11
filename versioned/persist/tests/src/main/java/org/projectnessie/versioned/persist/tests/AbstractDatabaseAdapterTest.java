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
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;

/**
 * Tests that verify {@link DatabaseAdapter} implementations. A few tests have similar pendants via
 * the tests against the {@code VersionStore}.
 */
@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterConfigItem(name = "max.key.list.size", value = "2048")
@NessieDbAdapterConfigItem(name = "global.log.entry.size", value = "2048")
@NessieDbAdapterConfigItem(name = "references.segment.size", value = "2048")
public abstract class AbstractDatabaseAdapterTest {
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;

  protected boolean commitWritesValidated() {
    return false;
  }

  @Nested
  public class CommitLogScan extends AbstractCommitLogScan {
    CommitLogScan() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class Events extends AbstractEvents {}

  @Nested
  public class Repositories extends AbstractRepositories {}

  @Nested
  public class RepoDescription extends AbstractRepoDescription {
    RepoDescription() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class CommitScenarios extends AbstractCommitScenarios {
    CommitScenarios() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class ManyCommits extends AbstractManyCommits {
    ManyCommits() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class WriteUpdateCommits extends AbstractWriteUpdateCommits {
    WriteUpdateCommits() {
      super(databaseAdapter, commitWritesValidated());
    }
  }

  @Nested
  public class ManyKeys extends AbstractManyKeys {
    ManyKeys() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class Concurrency extends AbstractConcurrency {
    Concurrency() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class RefLog extends AbstractRefLog {
    RefLog() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class MergeTransplant extends AbstractMergeTransplant {
    MergeTransplant() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class References extends AbstractReferences {
    References() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class GetNamedReferences extends AbstractGetNamedReferences {
    GetNamedReferences() {
      super(databaseAdapter);
    }
  }

  @Nested
  public class Diff extends AbstractDiff {
    Diff() {
      super(databaseAdapter);
    }
  }
}
