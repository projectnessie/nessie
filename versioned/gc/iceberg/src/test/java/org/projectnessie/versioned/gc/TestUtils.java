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
package org.projectnessie.versioned.gc;

import org.projectnessie.api.TreeApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

public final class TestUtils {
  private TestUtils() {}

  static void resetData(TreeApi tree) throws NessieConflictException, NessieNotFoundException {
    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch) {
        tree.deleteBranch(r.getName(), r.getHash());
      } else {
        tree.deleteTag(r.getName(), r.getHash());
      }
    }
    tree.createReference(null, Branch.of("main", null));
  }
}
