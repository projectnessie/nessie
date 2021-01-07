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
package com.dremio.nessie.versioned.store.dynamo;

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.attributeValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idValue;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConsumer.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoSerDe.deserialize;
import static com.dremio.nessie.versioned.store.dynamo.DynamoSerDe.deserializeToConsumer;
import static com.dremio.nessie.versioned.store.dynamo.DynamoSerDe.serializeWithConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.impl.MemoizedId;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.ConditionFailedException;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoStore implements Store {

  public static final int LOAD_SIZE = 100;

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoStore.class);

  private final int paginationSize = LOAD_SIZE;
  private final DynamoStoreConfig config;

  private DynamoDbClient client;
  private DynamoDbAsyncClient async;
  private final ImmutableMap<ValueType, String> tableNames;

  /**
   * create a DynamoStore.
   */
  public DynamoStore(DynamoStoreConfig config) {
    this.config = config;
    this.tableNames = Stream.of(ValueType.values())
        .collect(ImmutableMap.toImmutableMap(v -> v, v -> v.getTableName(config.getTablePrefix())));

    if (tableNames.size() != tableNames.values().stream().collect(Collectors.toSet()).size()) {
      throw new IllegalArgumentException("Each Nessie dynamo table must be named distinctly.");
    }
  }

  @Override
  public void start() {
    if (client != null && async != null) {
      return; // no-op
    }
    try {
      DynamoDbClientBuilder b1 = DynamoDbClient.builder();
      b1.httpClient(UrlConnectionHttpClient.create());
      DynamoDbAsyncClientBuilder b2 = DynamoDbAsyncClient.builder();
      b2.httpClient(NettyNioAsyncHttpClient.create());
      config.getEndpoint().ifPresent(ep -> {
        b1.endpointOverride(ep);
        b2.endpointOverride(ep);
      });

      config.getRegion().ifPresent(r -> {
        b1.region(r);
        b2.region(r);
      });

      client = b1.build();
      async = b2.build();

      if (config.initializeDatabase()) {
        Arrays.stream(ValueType.values())
          .map(tableNames::get)
          .collect(Collectors.toSet())
            .forEach(table -> createIfMissing(table));

        // make sure we have an empty l1 (ignore result, doesn't matter)
        putIfAbsent(ValueType.L1, L1.EMPTY);
        putIfAbsent(ValueType.L2, L2.EMPTY);
        putIfAbsent(ValueType.L3, L3.EMPTY);
      }

    } catch (DynamoDbException ex) {
      try {
        close();
      } catch (Exception e) {
        ex.addSuppressed(e);
      }
      throw new StoreOperationException("Failure connection to Dynamo", ex);
    }
  }

  @Override
  public void close() {
    Exception failure = null;
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        failure = e;
      } finally {
        client = null;
      }
    }
    if (async != null) {
      try {
        async.close();
      } catch (Exception e) {
        if (failure != null) {
          failure.addSuppressed(e);
        } else {
          failure = e;
        }
      } finally {
        async = null;
      }
    }
    if (failure != null) {
      throw new StoreOperationException("Failed to close store", failure);
    }
  }

  @Override
  public void load(LoadStep loadstep) throws NotFoundException {

    while (true) { // for each load step in the chain.
      List<ListMultimap<String, LoadOp<?>>> stepPages = paginateLoads(loadstep, paginationSize);

      for (ListMultimap<String, LoadOp<?>> l : stepPages) {
        Map<String, KeysAndAttributes> loads = l.keySet().stream().collect(Collectors.toMap(Function.identity(), table -> {
          List<LoadOp<?>> loadList = l.get(table);
          List<Map<String, AttributeValue>> keys = loadList.stream()
              .map(load -> ImmutableMap.of(KEY_NAME, idValue(load.getId())))
              .collect(Collectors.toList());
          return KeysAndAttributes.builder().keys(keys).consistentRead(true).build();
        }));

        BatchGetItemResponse response = client.batchGetItem(BatchGetItemRequest.builder().requestItems(loads).build());
        Map<String, List<Map<String, AttributeValue>>> responses = response.responses();
        Sets.SetView<String> missingElements = Sets.difference(loads.keySet(), responses.keySet());
        Preconditions.checkArgument(missingElements.isEmpty(), "Did not receive any objects for table(s) %s.", missingElements);

        for (String table : responses.keySet()) {
          List<LoadOp<?>> loadList = l.get(table);
          List<Map<String, AttributeValue>> values = responses.get(table);
          int missingResponses = loadList.size() - values.size();
          if (missingResponses != 0) {
            ValueType loadType = loadList.get(0).getValueType();
            if (loadType == ValueType.REF || loadType == ValueType.L1) {
              throw new NotFoundException("Unable to find requested ref.");
            }

            throw new NotFoundException(
                String.format("[%d] object(s) missing in table read [%s]. \n\nObjects expected: %s\n\nObjects Received: %s",
                missingResponses, table, loadList, responses));
          }

          // unfortunately, responses don't come in the order of the requests so we need to map between ids.
          Map<Id, LoadOp<? extends HasId>> opMap = loadList.stream().collect(Collectors.toMap(LoadOp::getId, Function.identity()));
          for (int i = 0; i < values.size(); i++) {
            Map<String, AttributeValue> item = values.get(i);
            ValueType valueType = ValueType.byValueName(attributeValue(item, ValueType.SCHEMA_TYPE).s());
            Id id = deserializeId(item, ID);
            LoadOp<? extends HasId> loadOp = opMap.get(id);
            if (loadOp == null) {
              throw new IllegalStateException("No load-op for loaded ID " + id);
            }
            loadOp.deserialize(consumer -> deserializeToConsumer(valueType, item, consumer));
          }
        }
      }
      Optional<LoadStep> next = loadstep.getNext();

      if (!next.isPresent()) {
        break;
      }
      loadstep = next.get();
    }
  }

  private List<ListMultimap<String, LoadOp<?>>> paginateLoads(LoadStep loadStep, int size) {

    List<LoadOp<?>> ops = loadStep.getOps().collect(Collectors.toList());

    List<ListMultimap<String, LoadOp<?>>> paginated = new ArrayList<>();
    for (int i = 0; i < ops.size(); i += size) {
      ListMultimap<String, LoadOp<?>> mm =
          Multimaps.index(ops.subList(i, Math.min(i + size, ops.size())), l -> tableNames.get(l.getValueType()));
      paginated.add(mm);
    }
    return paginated;
  }

  @Override
  public <V extends HasId> boolean putIfAbsent(ValueType type, V value) {
    ConditionExpression condition = ConditionExpression.of(ExpressionFunction.attributeNotExists(ExpressionPath.builder(KEY_NAME).build()));
    try {
      put(type, value, Optional.of(condition));
      return true;
    } catch (ConditionFailedException ex) {
      return false;
    }
  }

  /**
   * Delete all the tables within this store. For testing purposes only.
   */
  @VisibleForTesting
  public void deleteTables() {
    Arrays.stream(ValueType.values()).map(v -> tableNames.get(v)).collect(Collectors.toSet()).forEach(table -> {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(table).build());
      } catch (ResourceNotFoundException ex) {
        // ignore.
      }
    });

  }

  @Override
  public <V extends HasId> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    Preconditions.checkArgument(type.getObjectClass().isAssignableFrom(value.getClass()),
        "ValueType %s doesn't extend expected type %s.", value.getClass().getName(), type.getObjectClass().getName());
    @SuppressWarnings("rawtypes") MemoizedId v = (MemoizedId) value;
    @SuppressWarnings("Convert2MethodRef")
    Map<String, AttributeValue> attributes = serializeWithConsumer(type, cons -> v.applyToConsumer(cons));

    PutItemRequest.Builder builder = PutItemRequest.builder()
        .tableName(tableNames.get(type))
        .item(attributes);
    if (conditionUnAliased.isPresent()) {
      AliasCollectorImpl c = new AliasCollectorImpl();
      ConditionExpression aliased = conditionUnAliased.get().alias(c);
      c.apply(builder).conditionExpression(aliased.toConditionExpressionString());
    }

    try {
      client.putItem(builder.build());
    } catch (ConditionalCheckFailedException ex) {
      throw new ConditionFailedException("Condition failed during put operation.", ex);
    } catch (DynamoDbException ex) {
      throw new StoreOperationException("Failure during put.", ex);
    }
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    DeleteItemRequest.Builder delete = DeleteItemRequest.builder()
        .key(Collections.singletonMap(DynamoConsumer.ID, idValue(id)))
        .tableName(tableNames.get(type));

    AliasCollectorImpl collector = new AliasCollectorImpl();
    ConditionExpression aliased = addTypeCheck(type, condition).alias(collector);
    collector.apply(delete);
    delete.conditionExpression(aliased.toConditionExpressionString());

    try {
      client.deleteItem(delete.build());
      return true;
    } catch (ConditionalCheckFailedException ex) {
      LOGGER.debug("Failure during conditional check.", ex);
      return false;
    } catch (DynamoDbException ex) {
      throw new StoreOperationException("Failure during delete.", ex);
    }
  }

  private static ConditionExpression addTypeCheck(ValueType type, Optional<ConditionExpression> possibleExpression) {
    final ExpressionFunction checkType = ExpressionFunction.equals(
        ExpressionPath.builder(ValueType.SCHEMA_TYPE).build(),
        Entity.ofString(type.getValueName()));
    return possibleExpression.map(ce -> ce.and(checkType)).orElse(ConditionExpression.of(checkType));
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    List<CompletableFuture<BatchWriteItemResponse>> saves =  new ArrayList<>();
    for (int i = 0; i < ops.size(); i += paginationSize) {

      ListMultimap<String, SaveOp<?>> mm =
          Multimaps.index(ops.subList(i, Math.min(i + paginationSize, ops.size())), l -> tableNames.get(l.getType()));
      @SuppressWarnings("Convert2MethodRef")
      ListMultimap<String, WriteRequest> writes = Multimaps.transformValues(mm, save ->
          WriteRequest.builder().putRequest(PutRequest.builder().item(
              serializeWithConsumer(save.getType(), cons -> save.serialize(cons))).build())
                    .build());
      BatchWriteItemRequest batch = BatchWriteItemRequest.builder().requestItems(writes.asMap()).build();
      saves.add(async.batchWriteItem(batch));
    }

    try {
      CompletableFuture.allOf(saves.toArray(new CompletableFuture[0])).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof DynamoDbException) {
        throw new StoreOperationException("Dynamo failure during save.", e.getCause());
      } else {
        Throwables.throwIfUnchecked(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public <V extends HasId> V loadSingle(ValueType valueType, Id id) {
    Producer<V, ?> producer = valueType.newEntityProducer();
    HasIdConsumer consumer = producer;
    loadSingle(valueType, id, consumer);
    return producer.build();
  }

  @Override
  public <C extends HasIdConsumer<C>> void loadSingle(ValueType valueType, Id id, C consumer) {
    GetItemResponse response = client.getItem(GetItemRequest.builder()
        .tableName(tableNames.get(valueType))
        .key(ImmutableMap.of(KEY_NAME, idValue(id)))
        .consistentRead(true)
        .build());
    if (!response.hasItem()) {
      throw new NotFoundException("Unable to load item.");
    }
    deserializeToConsumer(valueType, response.item(), consumer);
  }

  @Override
  public <V extends HasId> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws NotFoundException {
    try {
      AliasCollectorImpl collector = new AliasCollectorImpl();
      UpdateExpression aliased = update.alias(collector);
      Optional<ConditionExpression> aliasedCondition = condition.map(e -> e.alias(collector));
      UpdateItemRequest.Builder updateRequest = collector.apply(UpdateItemRequest.builder())
          .returnValues(ReturnValue.ALL_NEW)
          .tableName(tableNames.get(type))
          .key(ImmutableMap.of(KEY_NAME, idValue(id)))
          .updateExpression(aliased.toUpdateExpressionString());
      aliasedCondition.ifPresent(e -> updateRequest.conditionExpression(e.toConditionExpressionString()));
      UpdateItemRequest builtRequest = updateRequest.build();
      UpdateItemResponse response = client.updateItem(builtRequest);
      return Optional.of(deserialize(type, response.attributes()));
    } catch (ResourceNotFoundException ex) {
      throw new NotFoundException("Unable to find value.", ex);
    } catch (ConditionalCheckFailedException checkFailed) {
      LOGGER.debug("Conditional check failed.", checkFailed);
      return Optional.empty();
    }
  }

  private final boolean tableExists(String name) {
    try {

      DescribeTableResponse refTable = client.describeTable(DescribeTableRequest.builder().tableName(name).build());
      verifyKeySchema(refTable.table());
      return true;
    } catch (ResourceNotFoundException e) {
      LOGGER.debug("Didn't find ref table, going to create one.", e);
      return false;
    }
  }


  @Override
  public <V extends HasId> Stream<V> getValues(Class<V> valueClass, ValueType type) {
    return client.scanPaginator(ScanRequest.builder().tableName(tableNames.get(type)).build())
        .stream()
        .flatMap(r -> r.items().stream())
        .map(i -> deserialize(type, i));
  }

  private final void createIfMissing(String name) {
    if (!tableExists(name)) {
      createTable(name);
    }
  }

  private final void createTable(String name) {
    client.createTable(CreateTableRequest.builder()
        .tableName(name)
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(KEY_NAME)
            .attributeType(
              ScalarAttributeType.B)
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(10L)
            .writeCapacityUnits(10L)
            .build())
        .keySchema(KeySchemaElement.builder()
            .attributeName(KEY_NAME)
            .keyType(KeyType.HASH)
            .build())
        .build());
  }

  private static final void verifyKeySchema(TableDescription description) {
    List<KeySchemaElement> elements = description.keySchema();

    if (elements.size() == 1) {
      KeySchemaElement key = elements.get(0);
      if (key.attributeName().equals(KEY_NAME)) {
        if (key.keyType() == KeyType.HASH) {
          return;
        }
      }
    }
    throw new IllegalStateException(String.format("Invalid key schema for table: %s. Key schema should be a hash partitioned "
        + "attribute with the name 'id'.", description.tableName()));
  }
}
