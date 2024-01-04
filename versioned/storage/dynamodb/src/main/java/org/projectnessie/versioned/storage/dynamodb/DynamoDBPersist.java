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
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyListIterator;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBBackend.condition;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBBackend.keyPrefix;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.BATCH_GET_LIMIT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_CONDITION_COMMON;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_CREATED_AT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_DELETED;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_POINTER;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.COL_REFERENCES_PREVIOUS;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.CONDITION_STORE_OBJ;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.CONDITION_STORE_REF;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.ITEM_SIZE_LIMIT;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBConstants.KEY_NAME;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;
import static software.amazon.awssdk.core.SdkBytes.fromByteArray;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBool;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.BEGINS_WITH;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.IN;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.dynamodb.serializers.ObjSerializer;
import org.projectnessie.versioned.storage.dynamodb.serializers.ObjSerializers;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class DynamoDBPersist implements Persist {

  private final DynamoDBBackend backend;
  private final StoreConfig config;
  private final String keyPrefix;

  DynamoDBPersist(DynamoDBBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
    this.keyPrefix = keyPrefix(config.repositoryId());
  }

  @Nonnull
  @Override
  public String name() {
    return DynamoDBBackendFactory.NAME;
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
    }
  }

  @Override
  @Nullable
  public Reference fetchReference(@Nonnull String name) {
    GetItemResponse item =
        backend.client().getItem(b -> b.tableName(backend.tableRefs).key(referenceKeyMap(name)));
    if (!item.hasItem()) {
      return null;
    }

    Map<String, AttributeValue> i = item.item();

    String createdAtStr = DynamoDBSerde.attributeToString(i, COL_REFERENCES_CREATED_AT);
    long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
    return reference(
        name,
        DynamoDBSerde.attributeToObjId(i, COL_REFERENCES_POINTER),
        DynamoDBSerde.attributeToBool(i, COL_REFERENCES_DELETED),
        createdAt,
        DynamoDBSerde.attributeToObjId(i, COL_REFERENCES_EXTENDED_INFO),
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

    BatchGetItemResponse response =
        backend.client().batchGetItem(b -> b.requestItems(requestItems));

    response
        .responses()
        .get(backend.tableRefs)
        .forEach(
            item -> {
              String name = item.get(KEY_NAME).s().substring(keyPrefix.length());
              String createdAtStr =
                  DynamoDBSerde.attributeToString(item, COL_REFERENCES_CREATED_AT);
              long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
              Reference reference =
                  reference(
                      name,
                      DynamoDBSerde.attributeToObjId(item, COL_REFERENCES_POINTER),
                      DynamoDBSerde.attributeToBool(item, COL_REFERENCES_DELETED),
                      createdAt,
                      DynamoDBSerde.attributeToObjId(item, COL_REFERENCES_EXTENDED_INFO),
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
  public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
    GetItemResponse item =
        backend.client().getItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    return itemToObj(item.item());
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(@Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    GetItemResponse item =
        backend.client().getItem(b -> b.tableName(backend.tableObjs).key(objKeyMap(id)));
    if (!item.hasItem()) {
      throw new ObjNotFoundException(id);
    }

    Obj obj = itemToObj(item.item());
    if (!obj.type().equals(type)) {
      throw new ObjNotFoundException(id);
    }

    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
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

    return ObjTypes.forShortName(item.item().get(COL_OBJ_TYPE).s());
  }

  @Nonnull
  @Override
  public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
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
              Obj obj = itemToObj(item);
              int idx = idToIndex.getValue(obj.id());
              if (idx != -1) {
                r[idx] = obj;
              }
            });
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
    }
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
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
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    // DynamoDB does not support "PUT IF NOT EXISTS" in a BatchWriteItemRequest/PutItem
    try (BatchWrite batchWrite = new BatchWrite(backend, backend.tableObjs)) {
      for (Obj obj : objs) {
        if (obj != null) {
          ObjId id = obj.id();
          checkArgument(id != null, "Obj to store must have a non-null ID");

          Map<String, AttributeValue> item = objToItem(obj, id, false);

          batchWrite.addPut(item);
        }
      }
    }
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

  private Obj itemToObj(Map<String, AttributeValue> item) {
    ObjId id = objIdFromString(item.get(KEY_NAME).s().substring(keyPrefix.length()));
    AttributeValue attributeValue = item.get(COL_OBJ_TYPE);
    ObjType type = ObjTypes.forShortName(attributeValue.s());
    ObjSerializer<?> serializer = ObjSerializers.forType(type);
    Map<String, AttributeValue> inner = item.get(serializer.attributeName()).m();
    return serializer.fromMap(id, type, inner);
  }

  @Nonnull
  private Map<String, AttributeValue> objToItem(
      @Nonnull Obj obj, ObjId id, boolean ignoreSoftSizeRestrictions) throws ObjTooLargeException {
    ObjType type = obj.type();
    ObjSerializer<Obj> serializer = ObjSerializers.forType(type);

    Map<String, AttributeValue> item = new HashMap<>();
    Map<String, AttributeValue> inner = new HashMap<>();
    item.put(KEY_NAME, objKey(id));
    item.put(COL_OBJ_TYPE, fromS(type.shortName()));
    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    serializer.toMap(obj, inner, incrementalIndexSizeLimit, indexSizeLimit);
    item.put(serializer.attributeName(), fromM(inner));
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
    DynamoDBSerde.objIdToAttribute(item, COL_REFERENCES_POINTER, reference.pointer());
    item.put(COL_REFERENCES_DELETED, fromBool(reference.deleted()));
    item.put(COL_REFERENCES_CREATED_AT, referencesCreatedAt(reference));
    DynamoDBSerde.objIdToAttribute(item, COL_REFERENCES_EXTENDED_INFO, reference.extendedInfoObj());

    byte[] previousPointers = serializePreviousPointers(reference.previousPointers());
    if (previousPointers != null) {
      item.put(COL_REFERENCES_PREVIOUS, fromB(fromByteArray(previousPointers)));
    }

    return item;
  }

  private void conditionalReferencePut(@Nonnull Reference reference, Reference expected) {
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
    DynamoDBSerde.objIdToAttribute(values, ":pointer", reference.pointer());
    values.put(":deleted", fromBool(reference.deleted()));
    values.put(":createdAt", referencesCreatedAt(reference));
    DynamoDBSerde.objIdToAttribute(values, ":extendedInfo", reference.extendedInfoObj());
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
        return itemToObj(item);
      }
    }

    @Override
    public void close() {}
  }
}
