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
package org.projectnessie.versioned.storage.versionstore;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.persist.ObjType.VALUE;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.store.DefaultStoreWorker;

public final class ContentMapping {

  static final StoreWorker STORE_WORKER = DefaultStoreWorker.instance();
  private final Persist persist;

  public ContentMapping(Persist persist) {
    this.persist = persist;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public Content fetchContent(@Nonnull @jakarta.annotation.Nonnull ObjId objId)
      throws ObjNotFoundException {
    ContentValueObj contentValue = persist.fetchTypedObj(objId, VALUE, ContentValueObj.class);
    return valueToContent(contentValue);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public Map<ContentKey, Content> fetchContents(
      @Nonnull @jakarta.annotation.Nonnull Map<ObjId, ContentKey> idsToKeys)
      throws ObjNotFoundException {
    Map<ContentKey, Content> r = new HashMap<>();
    ObjId[] ids = idsToKeys.keySet().toArray(new ObjId[0]);
    Obj[] objs = persist.fetchObjs(ids);
    for (int i = 0; i < ids.length; i++) {
      Obj obj = objs[i];
      if (obj instanceof ContentValueObj) {
        ContentValueObj contentValue = (ContentValueObj) obj;
        ContentKey key = idsToKeys.get(obj.id());
        Content content = valueToContent(contentValue);
        r.put(key, content);
      }
    }
    return r;
  }

  private static Content valueToContent(ContentValueObj contentValue) {
    return STORE_WORKER.valueFromStore((byte) contentValue.payload(), contentValue.data());
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public ContentValueObj buildContent(
      @Nonnull @jakarta.annotation.Nonnull Content putValue, int payload) {
    checkArgument(payload > 0 && payload <= 127, "payload must be > 0 and <= 127");
    String contentId = putValue.getId();
    checkArgument(contentId != null, "Content to store must have a non-null content ID");

    ByteString contentPut = STORE_WORKER.toStoreOnReferenceState(putValue);

    return contentValue(contentId, payload, contentPut);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public Content assignContentId(
      @Nonnull @jakarta.annotation.Nonnull Content putValue, String contentId) {
    return STORE_WORKER.applyId(putValue, contentId);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public Commit commitObjToCommit(
      boolean fetchAdditionalInfo, @Nonnull @jakarta.annotation.Nonnull CommitObj commitObj)
      throws ObjNotFoundException {
    return commitObjToCommit(fetchAdditionalInfo, commitObj, commitObj.id());
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public Commit commitObjToCommit(
      boolean fetchAdditionalInfo,
      @Nonnull @jakarta.annotation.Nonnull CommitObj commitObj,
      @Nonnull @jakarta.annotation.Nonnull ObjId commitId)
      throws ObjNotFoundException {
    ImmutableCommit.Builder commit =
        Commit.builder()
            .hash(objIdToHash(commitId))
            .parentHash(objIdToHash(commitObj.directParent()));

    CommitMeta commitMeta = toCommitMeta(commitObj);

    if (fetchAdditionalInfo) {
      IndexesLogic indexesLogic = indexesLogic(persist);
      List<ObjId> ids = new ArrayList<>();
      List<ContentKey> keys = new ArrayList<>();
      for (StoreIndexElement<CommitOp> op : indexesLogic.commitOperations(commitObj)) {
        ContentKey key = storeKeyToKey(op.key());
        // Note: key==null, if not the "main universe" or not a "content" discriminator
        if (key != null) {
          CommitOp c = op.content();
          if (c.action().exists()) {
            ObjId objId = requireNonNull(c.value(), "Required value pointer is null");
            ids.add(objId);
            keys.add(key);
          } else {
            commit.addOperations(Delete.of(key));
          }
        }
      }
      if (!ids.isEmpty()) {
        Obj[] objs = persist.fetchObjs(ids.toArray(new ObjId[0]));
        for (int i = 0; i < objs.length; i++) {
          Obj obj = objs[i];
          ContentKey key = keys.get(i);
          assert obj instanceof ContentValueObj;
          ContentValueObj contentValue = (ContentValueObj) obj;
          commit.addOperations(Put.of(key, contentValue.payload(), contentValue.data()));
        }
      }
    }

    return commit.commitMeta(commitMeta).build();
  }
}
