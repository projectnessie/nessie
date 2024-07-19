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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.STRING;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Builder;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class StringLogicImpl implements StringLogic {
  private final Persist persist;

  StringLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  public StringValue fetchString(ObjId stringObjId) throws ObjNotFoundException {
    return fetchString(persist.fetchTypedObj(stringObjId, STRING, StringObj.class));
  }

  @Override
  public StringValue fetchString(StringObj stringObj) {
    checkState(
        stringObj.compression() == Compression.NONE,
        "Unsupported compression %s",
        stringObj.compression());
    checkState(
        stringObj.predecessors().isEmpty(), "Predecessors in StringObj are not yet supported");

    return new StringValueHolder(stringObj);
  }

  @Override
  public StringObj updateString(
      StringValue previousValue, String contentType, byte[] stringValueUtf8) {
    // We currently ignore the previousValue here. It can be _later_ used to potentially store
    // diffs.

    ByteString text = unsafeWrap(stringValueUtf8);
    return stringData(contentType, Compression.NONE, null, emptyList(), text);
  }

  @Override
  public StringObj updateString(StringValue previousValue, String contentType, String stringValue) {
    return updateString(previousValue, contentType, stringValue.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public StringValue updateStringOnRef(
      Reference reference,
      StoreKey storeKey,
      Consumer<Builder> commitEnhancer,
      String contentType,
      byte[] stringValueUtf8)
      throws ObjNotFoundException,
          CommitConflictException,
          RefNotFoundException,
          RefConditionFailedException {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);
    CommitObj head = commitLogic.headCommit(reference);
    StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndexOrEmpty(head);
    StoreIndexElement<CommitOp> existingElement = index.get(storeKey);

    // If we are updating an existing string, reuse its content-id (which may be null,
    // e.g. for repo descriptions). Otherwise, generate a new content-id.
    ObjId existingValueId = null;
    UUID contentId = null;
    if (existingElement != null) {
      CommitOp op = existingElement.content();
      if (op.action().exists()) {
        existingValueId = op.value();
        contentId = op.contentId();
      }
    } else {
      contentId = UUID.randomUUID();
    }

    StringValue existing = existingValueId != null ? fetchString(existingValueId) : null;
    StringObj newValue = updateString(existing, contentType, stringValueUtf8);

    ObjId newValueId = requireNonNull(newValue.id());
    if (!newValueId.equals(existingValueId)) {
      CreateCommit.Builder builder =
          CreateCommit.newCommitBuilder()
              .parentCommitId(reference.pointer())
              .headers(EMPTY_COMMIT_HEADERS)
              .addAdds(commitAdd(storeKey, 0, newValueId, existingValueId, contentId));
      commitEnhancer.accept(builder);
      CommitObj committed = commitLogic.doCommit(builder.build(), singletonList(newValue));
      persist.updateReferencePointer(reference, requireNonNull(committed, "committed").id());
    }
    return existing;
  }

  static final class StringValueHolder implements StringValue {
    final StringObj obj;

    StringValueHolder(StringObj obj) {
      this.obj = obj;
    }

    @Override
    public String contentType() {
      return obj.contentType();
    }

    @Override
    public String completeValue() {
      return obj.text().toStringUtf8();
    }

    @Override
    public ObjId objId() {
      return obj.id();
    }
  }
}
