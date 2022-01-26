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
package org.projectnessie.versioned.tests;

import org.junit.jupiter.api.Nested;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.VersionStore;

/** Base class used for integration tests against version store implementations. */
public abstract class AbstractVersionStoreTestBase {

  protected abstract VersionStore<String, String, StringStoreWorker.TestEnum> store();

  @Nested
  public class Commits extends AbstractCommits {
    public Commits() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  public class Contents extends AbstractContents {
    public Contents() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  public class CommitLog extends AbstractCommitLog {
    public CommitLog() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  public class References extends AbstractReferences {
    public References() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  protected class Assign extends AbstractAssign {
    public Assign() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  protected class Transplant extends AbstractTransplant {
    public Transplant() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  protected class Merge extends AbstractMerge {
    public Merge() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  protected class Diff extends AbstractDiff {
    public Diff() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }

  @Nested
  public class ReferenceNotFound extends AbstractReferenceNotFound {
    public ReferenceNotFound() {
      super(AbstractVersionStoreTestBase.this.store());
    }
  }
}
