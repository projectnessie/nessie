/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.impl;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.catalog.model.ops.CatalogOperationResult;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;

/** Maintains state across all individual updates of a commit. */
final class MultiTableUpdate {
  private final CommitMultipleOperationsBuilder nessieCommit;
  private final List<SingleTableUpdate> tableUpdates = new ArrayList<>();
  private final List<String> storedLocations = new ArrayList<>();
  private Map<ContentKey, String> addedContentsMap;
  private Branch targetBranch;
  private boolean committed;

  MultiTableUpdate(CommitMultipleOperationsBuilder nessieCommit, Branch target) {
    this.nessieCommit = nessieCommit;
    this.targetBranch = target;
  }

  MultiTableUpdate commit() throws NessieConflictException, NessieNotFoundException {
    synchronized (this) {
      committed = true;
      if (!tableUpdates.isEmpty()) {
        CommitResponse commitResponse = nessieCommit.commitWithResponse();
        addedContentsMap =
            commitResponse.getAddedContents() != null
                ? commitResponse.toAddedContentsMap()
                : Map.of();
        targetBranch = commitResponse.getTargetBranch();
      }
      return this;
    }
  }

  Branch targetBranch() {
    synchronized (this) {
      return targetBranch;
    }
  }

  Map<ContentKey, String> addedContentsMap() {
    synchronized (this) {
      return addedContentsMap != null ? addedContentsMap : Map.of();
    }
  }

  List<SingleTableUpdate> tableUpdates() {
    synchronized (this) {
      return tableUpdates;
    }
  }

  List<String> storedLocations() {
    synchronized (this) {
      return storedLocations;
    }
  }

  void addUpdate(ContentKey key, SingleTableUpdate singleTableUpdate) {
    checkState(!committed, "Already committed");
    synchronized (this) {
      tableUpdates.add(singleTableUpdate);
      nessieCommit.operation(Operation.Put.of(key, singleTableUpdate.contentAfter));
    }
  }

  void addStoredLocation(String location) {
    checkState(!committed, "Already committed");
    synchronized (this) {
      storedLocations.add(location);
    }
  }

  final class SingleTableUpdate implements CatalogOperationResult {
    final NessieEntitySnapshot<?> snapshot;
    final Content contentBefore;
    final Content contentAfter;
    final CatalogOperation<?> op;

    SingleTableUpdate(
        NessieEntitySnapshot<?> snapshot,
        Content contentBefore,
        Content contentAfter,
        CatalogOperation<?> op) {
      this.snapshot = snapshot;
      this.contentBefore = contentBefore;
      this.contentAfter = contentAfter;
      this.op = op;
    }

    @Override
    public CatalogOperation<?> getOperation() {
      return op;
    }

    @Nullable
    @jakarta.annotation.Nullable
    @Override
    public Content getContentBefore() {
      return contentBefore;
    }

    @Nullable
    @jakarta.annotation.Nullable
    @Override
    public Content getContentAfter() {
      return contentAfter;
    }

    @Override
    public Branch getEffectiveBranch() {
      return targetBranch;
    }
  }
}
