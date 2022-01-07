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
package org.projectnessie.client.api;

import java.util.List;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.MutableReference;
import org.projectnessie.model.Operation;

/**
 * Request builder for "commit multiple operations".
 *
 * @since {@link NessieApiV1}
 */
public interface CommitMultipleOperationsBuilder
    extends OnMutableReferenceBuilder<CommitMultipleOperationsBuilder> {

  CommitMultipleOperationsBuilder commitMeta(CommitMeta commitMeta);

  CommitMultipleOperationsBuilder operations(List<Operation> operations);

  CommitMultipleOperationsBuilder operation(Operation operation);

  /**
   * Performs the commit operation(s) against the server. Prefer {@link #commitTo(MutableReference)}
   * instead of {@link #reference(MutableReference)} plus this method.
   *
   * @return the reference with the committed operations, so with the updated commit hash.
   */
  MutableReference commit() throws NessieNotFoundException, NessieConflictException;

  /**
   * Convenience method to return the reference to commit to with the new HEAD of the target branch.
   *
   * @param branch the branch to commit to
   * @param <T> reference type - either a {@link org.projectnessie.model.Branch} or {@link
   *     org.projectnessie.model.Transaction}
   * @return reference object pointing to the commit
   */
  default <T extends MutableReference> T commitTo(T branch)
      throws NessieConflictException, NessieNotFoundException {
    @SuppressWarnings("unchecked")
    T newHead = (T) reference(branch).commit();
    return newHead;
  }
}
