/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.versioned.storage.common.logic.ConsistencyLogic.CommitStatus.commitStatus;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class ConsistencyLogicImpl implements ConsistencyLogic {
  private final Persist persist;

  ConsistencyLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  public <R> R checkReference(Reference reference, CommitStatusCallback<R> callback) {
    R r = checkCommit(reference.pointer(), callback);

    for (Reference.PreviousPointer previousPointer : reference.previousPointers()) {
      checkCommit(previousPointer.pointer(), callback);
    }

    return r;
  }

  @Override
  public <R> R checkCommit(ObjId commitId, CommitStatusCallback<R> callback) {
    if (EMPTY_OBJ_ID.equals(commitId)) {
      return callback.commitCallback(commitStatus(commitId, null, true, true));
    }

    Set<ObjId> test = new HashSet<>();
    CommitObj commitObj = null;
    try {
      commitObj = persist.fetchTypedObj(commitId, StandardObjType.COMMIT, CommitObj.class);

      if (commitObj.referenceIndex() != null) {
        test.add(commitObj.referenceIndex());
      }
      for (IndexStripe stripe : commitObj.referenceIndexStripes()) {
        test.add(stripe.segment());
      }

      persist.fetchObjs(test.toArray(new ObjId[0]));
      test.clear();
    } catch (ObjNotFoundException notFound) {
      return callback.commitCallback(commitStatus(commitId, commitObj, false, false));
    }

    try {
      StoreIndex<CommitOp> index =
          indexesLogic(persist).buildCompleteIndex(commitObj, Optional.empty());
      for (StoreIndexElement<CommitOp> el : index) {
        CommitOp op = el.content();
        if (op.action().exists()) {
          test.add(op.value());
          if (test.size() == 50) {
            persist.fetchObjs(test.toArray(new ObjId[0]));
            test.clear();
          }
        }
      }

      if (!test.isEmpty()) {
        persist.fetchObjs(test.toArray(new ObjId[0]));
      }

      return callback.commitCallback(commitStatus(commitId, commitObj, true, true));
    } catch (ObjNotFoundException notFound) {
      return callback.commitCallback(commitStatus(commitId, commitObj, true, false));
    }
  }
}
