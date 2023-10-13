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
package org.projectnessie.versioned.storage.dynamodb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyListIterator;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBBackend.condition;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBBackend.keyPrefix;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.BATCH_GET_LIMIT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_CREATED;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_INCOMPLETE_INDEX;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_INCREMENTAL_INDEX;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_MESSAGE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_REFERENCE_INDEX;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_REFERENCE_INDEX_STRIPES;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_SECONDARY_PARENTS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_SEQ;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_TAIL;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_COMMIT_TYPE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_INDEX;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_INDEX_INDEX;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REF;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_CONDITION_COMMON;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_CREATED_AT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_DELETED;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_POINTER;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_PREVIOUS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REF_CREATED_AT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REF_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REF_INITIAL_POINTER;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REF_NAME;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_SEGMENTS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_SEGMENTS_STRIPES;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRING;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRING_COMPRESSION;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRING_CONTENT_TYPE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRING_FILENAME;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRING_PREDECESSORS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRING_TEXT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRIPES_FIRST_KEY;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRIPES_LAST_KEY;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_STRIPES_SEGMENT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_TAG;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_TAG_HEADERS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_TAG_MESSAGE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_TAG_SIGNATURE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_VALUE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_VALUE_CONTENT_ID;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_VALUE_DATA;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_VALUE_PAYLOAD;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.CONDITION_STORE_OBJ;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.CONDITION_STORE_REF;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.ITEM_SIZE_LIMIT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.KEY_NAME;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;
import static software.amazon.awssdk.core.SdkBytes.fromByteArray;
import static software.amazon.awssdk.core.SdkBytes.fromByteBuffer;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBool;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.BEGINS_WITH;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.IN;

import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class DynamoDBPersist implements Persist {

  private static final Map<ObjType, StoreObjDesc<?>> STORE_OBJ_TYPE = new EnumMap<>(ObjType.class);

  private final DynamoDBBackend backend;
  private final StoreConfig config;
  private final String keyPrefix;

  DynamoDBPersist(DynamoDBBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
    this.keyPrefix = keyPrefix(config.repositoryId());
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return DynamoDBBackendFactory.NAME;
  }

  @Override
  public int hardObjectSizeLimit() {
    return ITEM_SIZE_LIMIT;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public StoreConfig config() {
    return config;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    try {
      backend
          .client()
          .putItem(
              b ->
                  b.tableName(backend.tableRefs)
                      .conditionExpression(CONDITION_STORE_REF)
                      .item(referenceAttributeValues(reference)));
      return reference;
    } catch (ConditionalCheckFailedException e) {
      throw new RefAlreadyExistsException(fetchReference(reference.name()));
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      reference = reference.withDeleted(false);
      Reference asDeleted = reference.withDeleted(true);
      conditionalReferencePut(asDeleted, reference);
      return asDeleted;
    } catch (ConditionalCheckFailedException e) {
      Reference r = fetchReference(reference.name());
      if (r == null) {
        throw new RefNotFoundException(reference.name());
      }
      throw new RefConditionFailedException(r);
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference updateReferencePointer(
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      reference = reference.withDeleted(false);
      Reference bumpedReference = reference.forNewPointer(newPointer, config);
      conditionalReferencePut(bumpedReference, reference);
      return bumpedReference;
    } catch (ConditionalCheckFailedException e) {
      Reference r = fetchReference(reference.name());
      if (r == null) {
        throw new RefNotFoundException(reference.name());
      }
      throw new RefConditionFailedException(r);
    }
  }

  @Override
  public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(true);
    String condition = referenceCondition(reference);
    Map<String, AttributeValue> values = referenceConditionAttributes(reference);

    String refName = reference.name();
    try {
      backend
          .client()
          .deleteItem(
              b ->
                  b.tableName(backend.tableRefs)
                      .key(referenceKeyMap(refName))
                      .expressionAttributeValues(values)
                      .conditionExpression(condition));
    } catch (ConditionalCheckFailedException e) {
      Reference r = fetchReference(refName);
      if (r == null) {
        throw new RefNotFoundException(refName);
      }
      throw new RefConditionFailedException(r);
    }
  }

  @Override
  @Nullable
  @jakarta.annotation.Nullable
  public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
    GetItemResponse item =
        backend.client().getItem(b -> b.tableName(backend.tableRefs).key(referenceKeyMap(name)));
    if (!item.hasItem()) {
      return null;
    }

    Map<String, AttributeValue> i = item.item();

    String createdAtStr = attributeToString(i, COL_REFERENCES_CREATED_AT);
    long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
    return reference(
        name,
        attributeToObjId(i, COL_REFERENCES_POINTER),
        attributeToBool(i, COL_REFERENCES_DELETED),
        createdAt,
        attributeToObjId(i, COL_REFERENCES_EXTENDED_INFO),
        attributeToPreviousPointers(i));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
    List<Map<String, AttributeValue>> keys =
        new ArrayList<>(Math.min(names.length, BATCH_GET_LIMIT));
    Object2IntHashMap<String> nameToIndex =
        new Object2IntHashMap<>(200, Hashing.DEFAULT_LOAD_FACTOR, -1);
    Reference[] r = new Reference[names.length];
    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      if (name != null) {
        keys.add(referenceKeyMap(name));
        nameToIndex.put(name, i);

        if (keys.size() == BATCH_GET_LIMIT) {
          findReferencesPage(r, keys, nameToIndex);
          keys.clear();
          nameToIndex.clear();
        }
      }
    }

    if (!keys.isEmpty()) {
      findReferencesPage(r, keys, nameToIndex);
    }

    return r;
  }

  private void findReferencesPage(
      Reference[] r,
      List<Map<String, AttributeValue>> keys,
      Object2IntHashMap<String> nameToIndex) {
    Map<String, KeysAndAttributes> requestItems =
        singletonMap(backend.tableRefs, KeysAndAttributes.builder().keys(keys).build());

    BatchGetItemResponse response =
        backend.client().batchGetItem(b -> b.requestItems(requestItems));

    response
        .responses()
        .get(backend.tableRefs)
        .forEach(
            item -> {
              String name = item.get(KEY_NAME).s().substring(keyPrefix.length());
              String createdAtStr = attributeToString(item, COL_REFERENCES_CREATED_AT);
              long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
              Reference reference =
                  reference(
                      name,
                      attributeToObjId(item, COL_REFERENCES_POINTER),
                      attributeToBool(item, COL_REFERENCES_DELETED),
                      createdAt,
                      attributeToObjId(item, COL_REFERENCES_EXTENDED_INFO),
                      attributeToPreviousPointers(item));
              int idx = nameToIndex.getValue(name);
              if (idx >= 0) {
                r[idx] = reference;
              }
            });
  }

  private List<Reference.PreviousPointer> attributeToPreviousPointers(
      Map<String, AttributeValue> item) {
    AttributeValue attr = item.get(COL_REFERENCES_PREVIOUS);
    if (attr == null) {
      return emptyList();
    }
    return deserializePreviousPointers(attr.b().asByteArray());
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Obj fetchObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) throws ObjNotFoundException {
    GetItemResponse item =
        backend.client().getItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    return decomposeObj(item.item());
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull @jakarta.annotation.Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    GetItemResponse item =
        backend.client().getItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    Obj obj = decomposeObj(item.item());
    if (obj.type() != type) {
      throw new ObjNotFoundException(id);
    }

    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    GetItemResponse item =
        backend
            .client()
            .getItem(
                b ->
                    b.tableName(backend.tableObjs)
                        .key(objKeyMap(id))
                        .attributesToGet(COL_OBJ_TYPE));
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    return objTypeFromItem(item.item());
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
      throws ObjNotFoundException {
    List<Map<String, AttributeValue>> keys = new ArrayList<>(Math.min(ids.length, BATCH_GET_LIMIT));
    Object2IntHashMap<ObjId> idToIndex =
        new Object2IntHashMap<>(200, Hashing.DEFAULT_LOAD_FACTOR, -1);
    Obj[] r = new Obj[ids.length];
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null) {
        keys.add(objKeyMap(id));
        idToIndex.put(id, i);

        if (keys.size() == BATCH_GET_LIMIT) {
          fetchObjsPage(r, keys, idToIndex);
          keys.clear();
          idToIndex.clear();
        }
      }
    }

    if (!keys.isEmpty()) {
      fetchObjsPage(r, keys, idToIndex);
    }

    List<ObjId> notFound = null;
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null && r[i] == null) {
        if (notFound == null) {
          notFound = new ArrayList<>();
        }
        notFound.add(id);
      }
    }
    if (notFound != null) {
      throw new ObjNotFoundException(notFound);
    }

    return r;
  }

  private void fetchObjsPage(
      Obj[] r, List<Map<String, AttributeValue>> keys, Object2IntHashMap<ObjId> idToIndex) {

    Map<String, KeysAndAttributes> requestItems =
        singletonMap(backend.tableObjs, KeysAndAttributes.builder().keys(keys).build());

    BatchGetItemResponse response =
        backend.client().batchGetItem(b -> b.requestItems(requestItems));

    response
        .responses()
        .get(backend.tableObjs)
        .forEach(
            item -> {
              Obj obj = decomposeObj(item);
              int idx = idToIndex.getValue(obj.id());
              if (idx != -1) {
                r[idx] = obj;
              }
            });
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public boolean[] storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    // DynamoDB does not support "PUT IF NOT EXISTS" in a BatchWriteItemRequest/PutItem
    boolean[] r = new boolean[objs.length];
    for (int i = 0; i < objs.length; i++) {
      Obj o = objs[i];
      if (o != null) {
        r[i] = storeObj(o);
      }
    }
    return r;
  }

  @Override
  public boolean storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    Map<String, AttributeValue> item = objToItem(obj, id, ignoreSoftSizeRestrictions);

    try {
      backend
          .client()
          .putItem(
              b ->
                  b.tableName(backend.tableObjs)
                      .conditionExpression(CONDITION_STORE_OBJ)
                      .item(item));
    } catch (ConditionalCheckFailedException e) {
      return false;
    } catch (DynamoDbException e) {
      // Best effort to detect whether an object exceeded DynamoDB's hard item size limit of 400k.
      AwsErrorDetails errorDetails = e.awsErrorDetails();
      if (checkItemSizeExceeded(errorDetails)) {
        throw new ObjTooLargeException();
      }
      throw e;
    }

    return true;
  }

  @Override
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    backend.client().deleteItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    try (BatchWrite batchWrite = new BatchWrite(backend, backend.tableObjs)) {
      for (ObjId id : ids) {
        batchWrite.addDelete(objKey(id));
      }
    }
  }

  @Override
  public void upsertObj(@Nonnull @jakarta.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    Map<String, AttributeValue> item = objToItem(obj, id, false);

    try {
      backend.client().putItem(b -> b.tableName(backend.tableObjs).item(item));
    } catch (DynamoDbException e) {
      // Best effort to detect whether an object exceeded DynamoDB's hard item size limit of 400k.
      AwsErrorDetails errorDetails = e.awsErrorDetails();
      if (checkItemSizeExceeded(errorDetails)) {
        throw new ObjTooLargeException();
      }
      throw e;
    }
  }

  @Override
  public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    // DynamoDB does not support "PUT IF NOT EXISTS" in a BatchWriteItemRequest/PutItem
    try (BatchWrite batchWrite = new BatchWrite(backend, backend.tableObjs)) {
      for (Obj obj : objs) {
        ObjId id = obj.id();
        checkArgument(id != null, "Obj to store must have a non-null ID");

        Map<String, AttributeValue> item = objToItem(obj, id, false);

        batchWrite.addPut(item);
      }
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(
      @Nonnull @jakarta.annotation.Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes);
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  private ObjType objTypeFromItem(Map<String, AttributeValue> item) {
    return objTypeFromItem(item.get(COL_OBJ_TYPE));
  }

  private ObjType objTypeFromItem(AttributeValue attributeValue) {
    String shortType = attributeValue.s();
    return ObjType.fromShortName(shortType);
  }

  @SuppressWarnings("unchecked")
  private Obj decomposeObj(Map<String, AttributeValue> item) {
    ObjId id = objIdFromString(item.get(KEY_NAME).s().substring(keyPrefix.length()));
    ObjType type = objTypeFromItem(item);
    @SuppressWarnings("rawtypes")
    StoreObjDesc storeObj = STORE_OBJ_TYPE.get(type);
    checkState(storeObj != null, "Cannot deserialize object type %s", type);
    Map<String, AttributeValue> inner = item.get(storeObj.typeName).m();
    return storeObj.fromMap(id, inner);
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  @jakarta.annotation.Nonnull
  private Map<String, AttributeValue> objToItem(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, ObjId id, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjType type = obj.type();
    @SuppressWarnings("rawtypes")
    StoreObjDesc storeObj = STORE_OBJ_TYPE.get(type);
    checkArgument(storeObj != null, "Cannot serialize object type %s ", type);

    Map<String, AttributeValue> item = new HashMap<>();
    Map<String, AttributeValue> inner = new HashMap<>();
    item.put(KEY_NAME, objKey(id));
    item.put(COL_OBJ_TYPE, fromS(type.shortName()));
    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    storeObj.toMap(obj, inner, incrementalIndexSizeLimit, indexSizeLimit);
    item.put(storeObj.typeName, fromM(inner));
    return item;
  }

  private abstract static class StoreObjDesc<O extends Obj> {
    final String typeName;

    StoreObjDesc(String typeName) {
      this.typeName = typeName;
    }

    abstract void toMap(
        O obj, Map<String, AttributeValue> i, int incrementalIndexSize, int maxSerializedIndexSize)
        throws ObjTooLargeException;

    abstract O fromMap(ObjId id, Map<String, AttributeValue> i);
  }

  static {
    STORE_OBJ_TYPE.put(
        ObjType.COMMIT,
        new StoreObjDesc<CommitObj>(COL_COMMIT) {
          @Override
          void toMap(
              CommitObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize)
              throws ObjTooLargeException {
            i.put(COL_COMMIT_SEQ, fromS(Long.toString(obj.seq())));
            i.put(COL_COMMIT_CREATED, fromS(Long.toString(obj.created())));
            ObjId referenceIndex = obj.referenceIndex();
            if (referenceIndex != null) {
              objIdToAttribute(i, COL_COMMIT_REFERENCE_INDEX, referenceIndex);
            }
            i.put(COL_COMMIT_MESSAGE, fromS(obj.message()));
            objIdsAttribute(i, COL_COMMIT_TAIL, obj.tail());
            objIdsAttribute(i, COL_COMMIT_SECONDARY_PARENTS, obj.secondaryParents());

            ByteString index = obj.incrementalIndex();
            if (index.size() > incrementalIndexSize) {
              throw new ObjTooLargeException(index.size(), incrementalIndexSize);
            }
            bytesAttribute(i, COL_COMMIT_INCREMENTAL_INDEX, index);

            if (!obj.referenceIndexStripes().isEmpty()) {
              i.put(
                  COL_COMMIT_REFERENCE_INDEX_STRIPES, stripesAttrList(obj.referenceIndexStripes()));
            }

            Map<String, AttributeValue> headerMap = new HashMap<>();
            CommitHeaders headers = obj.headers();
            for (String s : headers.keySet()) {
              headerMap.put(
                  s,
                  fromL(
                      headers.getAll(s).stream()
                          .map(AttributeValue::fromS)
                          .collect(Collectors.toList())));
            }
            if (!headerMap.isEmpty()) {
              i.put(COL_COMMIT_HEADERS, fromM(headerMap));
            }
            i.put(COL_COMMIT_INCOMPLETE_INDEX, fromBool(obj.incompleteIndex()));
            i.put(COL_COMMIT_TYPE, fromS(obj.commitType().shortName()));
          }

          @Override
          CommitObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            CommitObj.Builder b =
                commitBuilder()
                    .id(id)
                    .seq(Long.parseLong(attributeToString(i, COL_COMMIT_SEQ)))
                    .created(Long.parseLong(attributeToString(i, COL_COMMIT_CREATED)))
                    .message(attributeToString(i, COL_COMMIT_MESSAGE))
                    .incrementalIndex(attributeToBytes(i, COL_COMMIT_INCREMENTAL_INDEX))
                    .incompleteIndex(attributeToBool(i, COL_COMMIT_INCOMPLETE_INDEX))
                    .commitType(CommitType.fromShortName(attributeToString(i, COL_COMMIT_TYPE)));
            AttributeValue v = i.get(COL_COMMIT_REFERENCE_INDEX);
            if (v != null) {
              b.referenceIndex(attributeToObjId(v));
            }

            fromStripesAttrList(
                i.get(COL_COMMIT_REFERENCE_INDEX_STRIPES), b::addReferenceIndexStripes);

            attributeToObjIds(i, COL_COMMIT_TAIL, b::addTail);
            attributeToObjIds(i, COL_COMMIT_SECONDARY_PARENTS, b::addSecondaryParents);

            CommitHeaders.Builder headers = newCommitHeaders();
            AttributeValue headerMap = i.get(COL_COMMIT_HEADERS);
            if (headerMap != null) {
              headerMap.m().forEach((k, l) -> l.l().forEach(hv -> headers.add(k, hv.s())));
            }
            b.headers(headers.build());

            return b.build();
          }
        });

    STORE_OBJ_TYPE.put(
        ObjType.REF,
        new StoreObjDesc<RefObj>(COL_REF) {
          @Override
          void toMap(
              RefObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize) {
            i.put(COL_REF_NAME, fromS(obj.name()));
            i.put(COL_REF_CREATED_AT, fromS(Long.toString(obj.createdAtMicros())));
            objIdToAttribute(i, COL_REF_INITIAL_POINTER, obj.initialPointer());
            ObjId extendedInfoObj = obj.extendedInfoObj();
            if (extendedInfoObj != null) {
              objIdToAttribute(i, COL_REF_EXTENDED_INFO, extendedInfoObj);
            }
          }

          @Override
          RefObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            String createdAtStr = attributeToString(i, COL_REFERENCES_CREATED_AT);
            long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
            return ref(
                id,
                attributeToString(i, COL_REF_NAME),
                attributeToObjId(i, COL_REF_INITIAL_POINTER),
                createdAt,
                attributeToObjId(i, COL_REF_EXTENDED_INFO));
          }
        });

    STORE_OBJ_TYPE.put(
        ObjType.VALUE,
        new StoreObjDesc<ContentValueObj>(COL_VALUE) {
          @Override
          void toMap(
              ContentValueObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize) {
            i.put(COL_VALUE_CONTENT_ID, fromS(obj.contentId()));
            i.put(COL_VALUE_PAYLOAD, fromS(Integer.toString(obj.payload())));
            bytesAttribute(i, COL_VALUE_DATA, obj.data());
          }

          @Override
          ContentValueObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            return contentValue(
                id,
                attributeToString(i, COL_VALUE_CONTENT_ID),
                Integer.parseInt(attributeToString(i, COL_VALUE_PAYLOAD)),
                attributeToBytes(i, COL_VALUE_DATA));
          }
        });

    STORE_OBJ_TYPE.put(
        ObjType.INDEX_SEGMENTS,
        new StoreObjDesc<IndexSegmentsObj>(COL_SEGMENTS) {
          @Override
          void toMap(
              IndexSegmentsObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize) {
            i.put(COL_SEGMENTS_STRIPES, stripesAttrList(obj.stripes()));
          }

          @Override
          IndexSegmentsObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            List<IndexStripe> stripes = new ArrayList<>();
            fromStripesAttrList(i.get(COL_SEGMENTS_STRIPES), stripes::add);
            return indexSegments(id, stripes);
          }
        });

    STORE_OBJ_TYPE.put(
        ObjType.INDEX,
        new StoreObjDesc<IndexObj>(COL_INDEX) {
          @Override
          void toMap(
              IndexObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize)
              throws ObjTooLargeException {
            ByteString index = obj.index();
            if (index.size() > maxSerializedIndexSize) {
              throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
            }
            bytesAttribute(i, COL_INDEX_INDEX, index);
          }

          @Override
          IndexObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            return index(id, attributeToBytes(i, COL_INDEX_INDEX));
          }
        });

    STORE_OBJ_TYPE.put(
        ObjType.TAG,
        new StoreObjDesc<TagObj>(COL_TAG) {
          @Override
          void toMap(
              TagObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize) {
            String message = obj.message();
            if (message != null) {
              i.put(COL_TAG_MESSAGE, fromS(message));
            }

            Map<String, AttributeValue> headerMap = new HashMap<>();
            CommitHeaders headers = obj.headers();
            if (headers != null) {
              for (String s : headers.keySet()) {
                headerMap.put(
                    s,
                    fromL(
                        headers.getAll(s).stream()
                            .map(AttributeValue::fromS)
                            .collect(Collectors.toList())));
              }
              if (!headerMap.isEmpty()) {
                i.put(COL_TAG_HEADERS, fromM(headerMap));
              }
            }

            ByteString signature = obj.signature();
            if (signature != null) {
              bytesAttribute(i, COL_TAG_SIGNATURE, signature);
            }
          }

          @Override
          TagObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            CommitHeaders tagHeaders = null;
            AttributeValue headerMap = i.get(COL_COMMIT_HEADERS);
            if (headerMap != null) {
              CommitHeaders.Builder headers = newCommitHeaders();
              headerMap.m().forEach((k, l) -> l.l().forEach(hv -> headers.add(k, hv.s())));
              tagHeaders = headers.build();
            }

            return tag(
                id,
                attributeToString(i, COL_TAG_MESSAGE),
                tagHeaders,
                attributeToBytes(i, COL_TAG_SIGNATURE));
          }
        });

    STORE_OBJ_TYPE.put(
        ObjType.STRING,
        new StoreObjDesc<StringObj>(COL_STRING) {
          @Override
          void toMap(
              StringObj obj,
              Map<String, AttributeValue> i,
              int incrementalIndexSize,
              int maxSerializedIndexSize) {
            String s = obj.contentType();
            if (s != null && !s.isEmpty()) {
              i.put(COL_STRING_CONTENT_TYPE, fromS(s));
            }
            i.put(COL_STRING_COMPRESSION, fromS(obj.compression().name()));
            s = obj.filename();
            if (s != null && !s.isEmpty()) {
              i.put(COL_STRING_FILENAME, fromS(s));
            }
            objIdsAttribute(i, COL_STRING_PREDECESSORS, obj.predecessors());
            bytesAttribute(i, COL_STRING_TEXT, obj.text());
          }

          @Override
          StringObj fromMap(ObjId id, Map<String, AttributeValue> i) {
            List<ObjId> predecessors = new ArrayList<>();
            attributeToObjIds(i, COL_STRING_PREDECESSORS, predecessors::add);
            return stringData(
                id,
                attributeToString(i, COL_STRING_CONTENT_TYPE),
                Compression.valueOf(attributeToString(i, COL_STRING_COMPRESSION)),
                attributeToString(i, COL_STRING_FILENAME),
                predecessors,
                attributeToBytes(i, COL_STRING_TEXT));
          }
        });
  }

  private static void fromStripesAttrList(AttributeValue attrList, Consumer<IndexStripe> consumer) {
    if (attrList != null) {
      for (AttributeValue seg : attrList.l()) {
        Map<String, AttributeValue> m = seg.m();
        consumer.accept(
            indexStripe(
                keyFromString(attributeToString(m, COL_STRIPES_FIRST_KEY)),
                keyFromString(attributeToString(m, COL_STRIPES_LAST_KEY)),
                attributeToObjId(m, COL_STRIPES_SEGMENT)));
      }
    }
  }

  private static AttributeValue stripesAttrList(List<IndexStripe> stripes) {
    List<AttributeValue> stripeAttr = new ArrayList<>();
    for (IndexStripe stripe : stripes) {
      Map<String, AttributeValue> sv = new HashMap<>();
      sv.put(COL_STRIPES_FIRST_KEY, fromS(stripe.firstKey().rawString()));
      sv.put(COL_STRIPES_LAST_KEY, fromS(stripe.lastKey().rawString()));
      objIdToAttribute(sv, COL_STRIPES_SEGMENT, stripe.segment());
      stripeAttr.add(fromM(sv));
    }
    return fromL(stripeAttr);
  }

  private static boolean checkItemSizeExceeded(AwsErrorDetails errorDetails) {
    return "DynamoDb".equals(errorDetails.serviceName())
        && "ValidationException".equals(errorDetails.errorCode())
        && errorDetails.errorMessage().toLowerCase(Locale.ROOT).contains("item size");
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private Map<String, AttributeValue> referenceAttributeValues(
      @Nonnull @jakarta.annotation.Nonnull Reference reference) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KEY_NAME, referenceKey(reference.name()));
    objIdToAttribute(item, COL_REFERENCES_POINTER, reference.pointer());
    item.put(COL_REFERENCES_DELETED, fromBool(reference.deleted()));
    item.put(COL_REFERENCES_CREATED_AT, referencesCreatedAt(reference));
    objIdToAttribute(item, COL_REFERENCES_EXTENDED_INFO, reference.extendedInfoObj());

    byte[] previousPointers = serializePreviousPointers(reference.previousPointers());
    if (previousPointers != null) {
      item.put(COL_REFERENCES_PREVIOUS, fromB(fromByteArray(previousPointers)));
    }

    return item;
  }

  private void conditionalReferencePut(
      @Nonnull @jakarta.annotation.Nonnull Reference reference, Reference expected) {
    String condition = referenceCondition(expected);
    Map<String, AttributeValue> values = referenceConditionAttributes(expected);

    backend
        .client()
        .putItem(
            b ->
                b.tableName(backend.tableRefs)
                    .conditionExpression(condition)
                    .expressionAttributeValues(values)
                    .item(referenceAttributeValues(reference)));
  }

  private static Map<String, AttributeValue> referenceConditionAttributes(Reference reference) {
    Map<String, AttributeValue> values = new HashMap<>();
    objIdToAttribute(values, ":pointer", reference.pointer());
    values.put(":deleted", fromBool(reference.deleted()));
    values.put(":createdAt", referencesCreatedAt(reference));
    objIdToAttribute(values, ":extendedInfo", reference.extendedInfoObj());
    return values;
  }

  private static String referenceCondition(Reference reference) {
    return COL_REFERENCES_CONDITION_COMMON
        + (reference.createdAtMicros() != 0L
            ? " AND (" + COL_REFERENCES_CREATED_AT + " = :createdAt)"
            : " AND attribute_not_exists(" + COL_REFERENCES_CREATED_AT + ")")
        + (reference.extendedInfoObj() != null
            ? " AND (" + COL_REFERENCES_EXTENDED_INFO + " = :extendedInfo)"
            : " AND attribute_not_exists(" + COL_REFERENCES_EXTENDED_INFO + ")");
  }

  private static AttributeValue referencesCreatedAt(Reference reference) {
    long createdAt = reference.createdAtMicros();
    return createdAt == 0L ? null : fromS(Long.toString(createdAt));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private AttributeValue referenceKey(@Nonnull @jakarta.annotation.Nonnull String reference) {
    return fromS(keyPrefix + reference);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private Map<String, AttributeValue> referenceKeyMap(
      @Nonnull @jakarta.annotation.Nonnull String reference) {
    return singletonMap(KEY_NAME, referenceKey(reference));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private AttributeValue objKey(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    return fromS(keyPrefix + id);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private Map<String, AttributeValue> objKeyMap(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    return singletonMap(KEY_NAME, objKey(id));
  }

  private static void attributeToObjIds(
      Map<String, AttributeValue> i, String n, Consumer<ObjId> receiver) {
    AttributeValue v = i.get(n);
    if (v != null) {
      v.l().stream().map(el -> objIdFromByteBuffer(el.b().asByteBuffer())).forEach(receiver);
    }
  }

  private static String attributeToString(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    return v != null ? v.s() : null;
  }

  private static ByteString attributeToBytes(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    return v != null ? unsafeWrap(v.b().asByteArrayUnsafe()) : null;
  }

  private static boolean attributeToBool(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    if (v == null) {
      return false;
    }
    Boolean b = v.bool();
    return b != null && b;
  }

  private static ObjId attributeToObjId(Map<String, AttributeValue> i, String n) {
    return attributeToObjId(i.get(n));
  }

  private static ObjId attributeToObjId(AttributeValue v) {
    return v == null ? null : objIdFromByteBuffer(v.b().asByteBuffer());
  }

  private static void objIdToAttribute(Map<String, AttributeValue> i, String n, ObjId id) {
    i.put(n, id != null ? fromB(fromByteBuffer(id.asByteBuffer())) : null);
  }

  private static void objIdsAttribute(Map<String, AttributeValue> i, String n, List<ObjId> l) {
    if (l == null || l.isEmpty()) {
      return;
    }
    i.put(
        n,
        fromL(
            l.stream()
                .map(ObjId::asByteBuffer)
                .map(SdkBytes::fromByteBuffer)
                .map(AttributeValue::fromB)
                .collect(Collectors.toList())));
  }

  private static void bytesAttribute(Map<String, AttributeValue> i, String n, ByteString b) {
    i.put(n, fromB(fromByteBuffer(b.asReadOnlyByteBuffer())));
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Iterator<ScanResponse> iter;
    private Iterator<Map<String, AttributeValue>> pageIter = emptyListIterator();

    public ScanAllObjectsIterator(Set<ObjType> returnedObjTypes) {

      AttributeValue[] objTypes =
          returnedObjTypes.stream()
              .map(ObjType::shortName)
              .map(AttributeValue::fromS)
              .toArray(AttributeValue[]::new);
      Map<String, Condition> scanFilter = new HashMap<>();
      scanFilter.put(KEY_NAME, condition(BEGINS_WITH, fromS(keyPrefix)));
      scanFilter.put(COL_OBJ_TYPE, condition(IN, objTypes));

      iter =
          backend
              .client()
              .scanPaginator(b -> b.tableName(backend.tableObjs).scanFilter(scanFilter))
              .iterator();
    }

    @Override
    protected Obj computeNext() {
      while (true) {
        if (!pageIter.hasNext()) {
          if (!iter.hasNext()) {
            return endOfData();
          }
          ScanResponse r = iter.next();
          pageIter = r.items().iterator();
          continue;
        }

        Map<String, AttributeValue> item = pageIter.next();
        return decomposeObj(item);
      }
    }

    @Override
    public void close() {}
  }
}
