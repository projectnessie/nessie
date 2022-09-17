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
package org.projectnessie.gc.repository;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;

/** Interface providing the functionality to access the catalog/repository. */
public interface RepositoryConnector extends AutoCloseable {

  /** Retrieve all references to walk. References are usually walked in parallel. */
  Stream<Reference> allReferences() throws NessieNotFoundException;

  /** Retrieve the commit log of a single reference returned via {@link #allReferences()}. */
  Stream<LogResponse.LogEntry> commitLog(Reference ref) throws NessieNotFoundException;

  /**
   * Retrieves all remaining contents at the last live commit (the commit right before the first
   * non-live commit).
   */
  Stream<Map.Entry<ContentKey, Content>> allContents(Detached ref, Set<Content.Type> types)
      throws NessieNotFoundException;
}
