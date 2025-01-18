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
package org.projectnessie.versioned.storage.dynamodb2;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyListIterator;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjTypes.objTypeByName;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Backend.condition;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Backend.keyPrefix;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.BATCH_GET_LIMIT;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_OBJ_REFERENCED;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_OBJ_VALUE;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_OBJ_VERS;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_REFERENCES_CONDITION_COMMON;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_REFERENCES_CREATED_AT;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_REFERENCES_DELETED;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_REFERENCES_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_REFERENCES_POINTER;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.COL_REFERENCES_PREVIOUS;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.CONDITION_STORE_OBJ;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.CONDITION_STORE_REF;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.ITEM_SIZE_LIMIT;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.KEY_NAME;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Serde.attributeToString;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;
import static software.amazon.awssdk.core.SdkBytes.fromByteArray;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBool;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.BEGINS_WITH;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.IN;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class DynamoDB2Persist implements Persist {

  private final DynamoDB2Backend backend;
  private final StoreConfig config;
  private final String keyPrefix;

  DynamoDB2Persist(DynamoDB2Backend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
    this.keyPrefix = keyPrefix(config.repositoryId());
  }

  @Nonnull
  @Override
  public String name() {
    return DynamoDB2BackendFactory.NAME;
  }

  @Override
  public int hardObjectSizeLimit() {
    return ITEM_SIZE_LIMIT;
  }

  @Nonnull
  @Override
  public StoreConfig config() {
    return config;
  }

  @Nonnull
  @Override
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
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
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
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
  @Override
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
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
  public void purgeReference(@Nonnull Reference reference)
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
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  @Nullable
  public Reference fetchReference(@Nonnull String name) {
    GetItemResponse item;
    try {
      item =
          backend.client().getItem(b -> b.tableName(backend.tableRefs).key(referenceKeyMap(name)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    if (!item.hasItem()) {
      return null;
    }

    Map<String, AttributeValue> i = item.item();

    String createdAtStr = attributeToString(i, COL_REFERENCES_CREATED_AT);
    long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
    return reference(
        name,
        DynamoDB2Serde.attributeToObjId(i, COL_REFERENCES_POINTER),
        DynamoDB2Serde.attributeToBool(i, COL_REFERENCES_DELETED),
        createdAt,
        DynamoDB2Serde.attributeToObjId(i, COL_REFERENCES_EXTENDED_INFO),
        attributeToPreviousPointers(i));
  }

  @Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull String[] names) {
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

    try {
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
                        DynamoDB2Serde.attributeToObjId(item, COL_REFERENCES_POINTER),
                        DynamoDB2Serde.attributeToBool(item, COL_REFERENCES_DELETED),
                        createdAt,
                        DynamoDB2Serde.attributeToObjId(item, COL_REFERENCES_EXTENDED_INFO),
                        attributeToPreviousPointers(item));
                int idx = nameToIndex.getValue(name);
                if (idx >= 0) {
                  r[idx] = reference;
                }
              });
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
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
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    GetItemResponse item;
    try {
      item = backend.client().getItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    T obj = itemToObj(item.item(), type, typeClass);
    if (obj == null) {
      throw new ObjNotFoundException(id);
    }
    return obj;
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    GetItemResponse item;
    try {
      item =
          backend
              .client()
              .getItem(
                  b ->
                      b.tableName(backend.tableObjs)
                          .key(objKeyMap(id))
                          .attributesToGet(COL_OBJ_TYPE));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    return objTypeByName(item.item().get(COL_OBJ_TYPE).s());
  }

  @Nonnull
  @Override
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    List<Map<String, AttributeValue>> keys = new ArrayList<>(Math.min(ids.length, BATCH_GET_LIMIT));
    Object2IntHashMap<ObjId> idToIndex =
        new Object2IntHashMap<>(200, Hashing.DEFAULT_LOAD_FACTOR, -1);
    @SuppressWarnings("unchecked")
    T[] r = (T[]) Array.newInstance(typeClass, ids.length);
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null) {
        keys.add(objKeyMap(id));
        idToIndex.put(id, i);

        if (keys.size() == BATCH_GET_LIMIT) {
          fetchObjsPage(r, keys, idToIndex, type, typeClass);
          keys.clear();
          idToIndex.clear();
        }
      }
    }

    if (!keys.isEmpty()) {
      fetchObjsPage(r, keys, idToIndex, type, typeClass);
    }

    return r;
  }

  private <T extends Obj> void fetchObjsPage(
      T[] r,
      List<Map<String, AttributeValue>> keys,
      Object2IntHashMap<ObjId> idToIndex,
      ObjType type,
      Class<T> typeClass) {

    Map<String, KeysAndAttributes> requestItems =
        singletonMap(backend.tableObjs, KeysAndAttributes.builder().keys(keys).build());

    try {
      BatchGetItemResponse response =
          backend.client().batchGetItem(b -> b.requestItems(requestItems));

      response
          .responses()
          .get(backend.tableObjs)
          .forEach(
              item -> {
                T obj = itemToObj(item, type, typeClass);
                if (obj != null) {
                  int idx = idToIndex.getValue(obj.id());
                  if (idx != -1) {
                    r[idx] = obj;
                  }
                }
              });
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Nonnull
  @Override
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
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
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    long referenced = config.currentTimeMicros();

    Map<String, AttributeValue> item = objToItem(obj, referenced, id, ignoreSoftSizeRestrictions);

    try {
      try {
        backend
            .client()
            .putItem(
                b ->
                    b.tableName(backend.tableObjs)
                        .conditionExpression(CONDITION_STORE_OBJ)
                        .item(item));
      } catch (ConditionalCheckFailedException e) {
        backend
            .client()
            .updateItem(
                b ->
                    b.tableName(backend.tableObjs)
                        .key(objKeyMap(id))
                        .attributeUpdates(
                            Map.of(
                                COL_OBJ_REFERENCED,
                                AttributeValueUpdate.builder()
                                    .value(fromS(Long.toString(referenced)))
                                    .build())));
        return false;
      }
    } catch (DynamoDbException e) {
      // Best effort to detect whether an object exceeded DynamoDB's hard item size limit of 400k.
      AwsErrorDetails errorDetails = e.awsErrorDetails();
      if (checkItemSizeExceeded(errorDetails)) {
        throw new ObjTooLargeException();
      }
      throw unhandledException(e);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    return true;
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    backend.client().deleteItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    try (BatchWrite batchWrite = new BatchWrite(backend, backend.tableObjs)) {
      for (ObjId id : ids) {
        if (id != null) {
          batchWrite.addDelete(objKey(id));
        }
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    long referenced = config.currentTimeMicros();
    obj = obj.withReferenced(referenced);

    Map<String, AttributeValue> item = objToItem(obj, config.currentTimeMicros(), id, false);

    try {
      backend.client().putItem(b -> b.tableName(backend.tableObjs).item(item));
    } catch (DynamoDbException e) {
      // Best effort to detect whether an object exceeded DynamoDB's hard item size limit of 400k.
      AwsErrorDetails errorDetails = e.awsErrorDetails();
      if (checkItemSizeExceeded(errorDetails)) {
        throw new ObjTooLargeException();
      }
      throw unhandledException(e);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    // DynamoDB does not support "PUT IF NOT EXISTS" in a BatchWriteItemRequest/PutItem
    try (BatchWrite batchWrite = new BatchWrite(backend, backend.tableObjs)) {
      long referenced = config.currentTimeMicros();
      for (Obj obj : objs) {
        if (obj != null) {
          ObjId id = obj.id();
          checkArgument(id != null, "Obj to store must have a non-null ID");

          obj = obj.withReferenced(referenced);

          Map<String, AttributeValue> item = objToItem(obj, config.currentTimeMicros(), id, false);

          batchWrite.addPut(item);
        }
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    ObjId id = obj.id();

    var deleteItemRequest =
        DeleteItemRequest.builder().tableName(backend.tableObjs).key(objKeyMap(id));
    if (obj.referenced() != -1L) {
      // We take a risk here in case the given object does _not_ have a referenced() value
      // (old object). It's not possible in DynamoDB to check for '== 0 OR IS ABSENT/NULL'.
      deleteItemRequest.expected(
          Map.of(
              COL_OBJ_REFERENCED,
              ExpectedAttributeValue.builder()
                  .value(fromS(Long.toString(obj.referenced())))
                  .build()));
    }

    try {
      backend.client().deleteItem(deleteItemRequest.build());
      return true;
    } catch (ConditionalCheckFailedException checkFailedException) {
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    ObjId id = obj.id();

    Map<String, ExpectedAttributeValue> expectedValues = conditionalUpdateExpectedValues(obj);

    try {
      backend
          .client()
          .deleteItem(
              b -> b.tableName(backend.tableObjs).key(objKeyMap(id)).expected(expectedValues));
      return true;
    } catch (ConditionalCheckFailedException checkFailedException) {
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    ObjId id = expected.id();

    checkArgument(id != null && id.equals(newValue.id()));
    checkArgument(expected.type().equals(newValue.type()));
    checkArgument(!expected.versionToken().equals(newValue.versionToken()));

    Map<String, ExpectedAttributeValue> expectedValues = conditionalUpdateExpectedValues(expected);

    long referenced = config.currentTimeMicros();
    Map<String, AttributeValueUpdate> updates =
        objToItem(newValue.withReferenced(referenced), config.currentTimeMicros(), id, false)
            .entrySet()
            .stream()
            .filter(e -> !COL_OBJ_TYPE.equals(e.getKey()) && !KEY_NAME.equals(e.getKey()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> AttributeValueUpdate.builder().value(e.getValue()).build()));

    try {
      // Although '.attributeUpdates()' + '.expected()' are deprecated those functionalities do
      // work, unlike the recommended expression(s) approach, which fails with HTTP/550 for our
      // 'updateConditional()'.
      backend
          .client()
          .updateItem(
              b ->
                  b.tableName(backend.tableObjs)
                      .key(objKeyMap(id))
                      .attributeUpdates(updates)
                      .expected(expectedValues));
      return true;
    } catch (ConditionalCheckFailedException checkFailedException) {
      return false;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  private static Map<String, ExpectedAttributeValue> conditionalUpdateExpectedValues(
      UpdateableObj expected) {
    Map<String, ExpectedAttributeValue> expectedValues = new HashMap<>();
    expectedValues.put(
        COL_OBJ_TYPE,
        ExpectedAttributeValue.builder().value(fromS(expected.type().shortName())).build());
    expectedValues.put(
        COL_OBJ_VERS,
        ExpectedAttributeValue.builder().value(fromS(expected.versionToken())).build());
    return expectedValues;
  }

  @Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes);
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  private <T extends Obj> T itemToObj(
      Map<String, AttributeValue> item, ObjType t, @SuppressWarnings("unused") Class<T> typeClass) {
    ObjId id = objIdFromString(item.get(KEY_NAME).s().substring(keyPrefix.length()));
    AttributeValue attributeValue = item.get(COL_OBJ_TYPE);
    ObjType type = objTypeByName(attributeValue.s());
    if (t != null && !t.equals(type)) {
      return null;
    }
    ByteBuffer bin = item.get(COL_OBJ_VALUE).b().asByteBuffer();
    String versionToken = attributeToString(item, COL_OBJ_VERS);
    String referencedString = attributeToString(item, COL_OBJ_REFERENCED);
    long referenced = referencedString != null ? Long.parseLong(referencedString) : -1L;
    Obj obj = deserializeObj(id, referenced, bin, versionToken);
    return typeClass.cast(obj);
  }

  @Nonnull
  private Map<String, AttributeValue> objToItem(
      @Nonnull Obj obj, long referenced, ObjId id, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjType type = obj.type();

    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KEY_NAME, objKey(id));
    item.put(COL_OBJ_TYPE, fromS(type.shortName()));
    if (obj.referenced() != -1) {
      // -1 is a sentinel for AbstractBasePersistTests.deleteWithReferenced()
      item.put(COL_OBJ_REFERENCED, fromS(Long.toString(referenced)));
    }
    UpdateableObj.extractVersionToken(obj).ifPresent(token -> item.put(COL_OBJ_VERS, fromS(token)));
    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    byte[] serialized = serializeObj(obj, incrementalIndexSizeLimit, indexSizeLimit, false);
    item.put(COL_OBJ_VALUE, fromB(fromByteArray(serialized)));
    return item;
  }

  private static boolean checkItemSizeExceeded(AwsErrorDetails errorDetails) {
    return "DynamoDb".equals(errorDetails.serviceName())
        && "ValidationException".equals(errorDetails.errorCode())
        && errorDetails.errorMessage().toLowerCase(Locale.ROOT).contains("item size");
  }

  @Nonnull
  private Map<String, AttributeValue> referenceAttributeValues(@Nonnull Reference reference) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KEY_NAME, referenceKey(reference.name()));
    DynamoDB2Serde.objIdToAttribute(item, COL_REFERENCES_POINTER, reference.pointer());
    item.put(COL_REFERENCES_DELETED, fromBool(reference.deleted()));
    item.put(COL_REFERENCES_CREATED_AT, referencesCreatedAt(reference));
    DynamoDB2Serde.objIdToAttribute(
        item, COL_REFERENCES_EXTENDED_INFO, reference.extendedInfoObj());

    byte[] previousPointers = serializePreviousPointers(reference.previousPointers());
    if (previousPointers != null) {
      item.put(COL_REFERENCES_PREVIOUS, fromB(fromByteArray(previousPointers)));
    }

    return item;
  }

  private void conditionalReferencePut(@Nonnull Reference reference, Reference expected) {
    String condition = referenceCondition(expected);
    Map<String, AttributeValue> values = referenceConditionAttributes(expected);

    try {
      backend
          .client()
          .putItem(
              b ->
                  b.tableName(backend.tableRefs)
                      .conditionExpression(condition)
                      .expressionAttributeValues(values)
                      .item(referenceAttributeValues(reference)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  private static Map<String, AttributeValue> referenceConditionAttributes(Reference reference) {
    Map<String, AttributeValue> values = new HashMap<>();
    DynamoDB2Serde.objIdToAttribute(values, ":pointer", reference.pointer());
    values.put(":deleted", fromBool(reference.deleted()));
    values.put(":createdAt", referencesCreatedAt(reference));
    DynamoDB2Serde.objIdToAttribute(values, ":extendedInfo", reference.extendedInfoObj());
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
  private AttributeValue referenceKey(@Nonnull String reference) {
    return fromS(keyPrefix + reference);
  }

  @Nonnull
  private Map<String, AttributeValue> referenceKeyMap(@Nonnull String reference) {
    return singletonMap(KEY_NAME, referenceKey(reference));
  }

  @Nonnull
  private AttributeValue objKey(@Nonnull ObjId id) {
    return fromS(keyPrefix + id);
  }

  @Nonnull
  private Map<String, AttributeValue> objKeyMap(@Nonnull ObjId id) {
    return singletonMap(KEY_NAME, objKey(id));
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Iterator<ScanResponse> iter;
    private Iterator<Map<String, AttributeValue>> pageIter = emptyListIterator();

    public ScanAllObjectsIterator(Set<ObjType> returnedObjTypes) {

      Map<String, Condition> scanFilter = new HashMap<>();
      scanFilter.put(KEY_NAME, condition(BEGINS_WITH, fromS(keyPrefix)));

      if (!returnedObjTypes.isEmpty()) {
        AttributeValue[] objTypes =
            returnedObjTypes.stream()
                .map(ObjType::shortName)
                .map(AttributeValue::fromS)
                .toArray(AttributeValue[]::new);
        scanFilter.put(COL_OBJ_TYPE, condition(IN, objTypes));
      }

      try {
        iter =
            backend
                .client()
                .scanPaginator(b -> b.tableName(backend.tableObjs).scanFilter(scanFilter))
                .iterator();
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
    }

    @Override
    protected Obj computeNext() {
      try {
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
          return itemToObj(item, null, Obj.class);
        }
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
    }

    @Override
    public void close() {}
  }

  static RuntimeException unhandledException(RuntimeException e) {
    if (e instanceof SdkException) {
      if (((SdkException) e).retryable()
          || e instanceof ApiCallTimeoutException
          || e instanceof ApiCallAttemptTimeoutException
          || e instanceof AbortedException) {
        return new UnknownOperationResultException(e);
      }
    }
    if (e instanceof AwsServiceException) {
      if (((AwsServiceException) e).isThrottlingException()) {
        return new UnknownOperationResultException(e);
      }
    }
    return e;
  }
}
