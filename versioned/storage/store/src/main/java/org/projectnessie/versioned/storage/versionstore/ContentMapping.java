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
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
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
  public Content fetchContent(@Nonnull ObjId objId) throws ObjNotFoundException {
    ContentValueObj contentValue = persist.fetchTypedObj(objId, VALUE, ContentValueObj.class);
    return valueToContent(contentValue);
  }

  @Nonnull
  public Map<ContentKey, Content> fetchContents(
      @Nonnull StoreIndex<CommitOp> index, @Nonnull Collection<ContentKey> keys)
      throws ObjNotFoundException {

    // Eagerly bulk-(pre)fetch the requested keys
    index.loadIfNecessary(
        keys.stream().map(TypeMapping::keyToStoreKey).collect(Collectors.toSet()));

    Map<ObjId, ContentKey> idsToKeys = newHashMapWithExpectedSize(keys.size());
    for (ContentKey key : keys) {
      ObjId valueObjId = valueObjIdByKey(key, index);
      if (valueObjId != null && idsToKeys.putIfAbsent(valueObjId, key) != null) {
        // There is a *very* low chance (only for old, migrated repositories) that the exact same
        // content object is used for multiple content-keys. If that situation happens, restart with
        // a special, slower and more expensive implementation.
        // It could have only happened before the enforcement of new content-IDs for Put-operations
        // was in place _and_ if the content-ID was explicitly specified by a client.
        return fetchContentsDuplicateObjIds(index, keys);
      }
    }

    ObjId[] ids = idsToKeys.keySet().toArray(new ObjId[0]);
    Obj[] objs = persist.fetchObjs(ids);
    Map<ContentKey, Content> r = newHashMapWithExpectedSize(ids.length);
    for (int i = 0; i < ids.length; i++) {
      Obj obj = objs[i];
      if (obj instanceof ContentValueObj) {
        ContentValueObj contentValue = (ContentValueObj) obj;
        ContentKey contentKey = idsToKeys.get(obj.id());
        Content content = valueToContent(contentValue);
        r.put(contentKey, content);
      }
    }
    return r;
  }

  private Map<ContentKey, Content> fetchContentsDuplicateObjIds(
      StoreIndex<CommitOp> index, Collection<ContentKey> keys) throws ObjNotFoundException {
    Map<ObjId, List<ContentKey>> idsToKeys = newHashMapWithExpectedSize(keys.size());
    for (ContentKey key : keys) {
      ObjId valueObjId = valueObjIdByKey(key, index);
      if (valueObjId != null) {
        idsToKeys.computeIfAbsent(valueObjId, x -> new ArrayList<>()).add(key);
      }
    }

    ObjId[] ids = idsToKeys.keySet().toArray(new ObjId[0]);
    Obj[] objs = persist.fetchObjs(ids);
    Map<ContentKey, Content> r = newHashMapWithExpectedSize(ids.length);
    for (int i = 0; i < ids.length; i++) {
      Obj obj = objs[i];
      if (obj instanceof ContentValueObj) {
        ContentValueObj contentValue = (ContentValueObj) obj;
        Content content = valueToContent(contentValue);
        for (ContentKey key : idsToKeys.get(obj.id())) {
          r.put(key, content);
        }
      }
    }
    return r;
  }

  private static ObjId valueObjIdByKey(ContentKey key, StoreIndex<CommitOp> index) {
    StoreKey storeKey = keyToStoreKey(key);
    StoreIndexElement<CommitOp> indexElement = index.get(storeKey);
    if (indexElement == null || !indexElement.content().action().exists()) {
      return null;
    }

    return requireNonNull(indexElement.content().value(), "Required value pointer is null");
  }

  private static Content valueToContent(ContentValueObj contentValue) {
    return STORE_WORKER.valueFromStore((byte) contentValue.payload(), contentValue.data());
  }

  @Nonnull
  public ContentValueObj buildContent(@Nonnull Content putValue, int payload) {
    checkArgument(payload > 0 && payload <= 127, "payload must be > 0 and <= 127");
    String contentId = putValue.getId();
    checkArgument(contentId != null, "Content to store must have a non-null content ID");

    ByteString contentPut = STORE_WORKER.toStoreOnReferenceState(putValue);

    return contentValue(contentId, payload, contentPut);
  }

  @Nonnull
  public Commit commitObjToCommit(boolean fetchAdditionalInfo, @Nonnull CommitObj commitObj)
      throws ObjNotFoundException {
    return commitObjToCommit(fetchAdditionalInfo, commitObj, commitObj.id());
  }

  @Nonnull
  public Commit commitObjToCommit(
      boolean fetchAdditionalInfo, @Nonnull CommitObj commitObj, @Nonnull ObjId commitId)
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
          commit.addOperations(
              Put.of(
                  key,
                  DefaultStoreWorker.instance()
                      .valueFromStore(contentValue.payload(), contentValue.data())));
        }
      }
    }

    return commit.commitMeta(commitMeta).build();
  }
}
