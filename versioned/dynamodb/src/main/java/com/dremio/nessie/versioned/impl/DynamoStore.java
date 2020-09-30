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
package com.dremio.nessie.versioned.impl;

import java.util.ArrayList;
import java.util.Arrays;
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

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.condition.AliasCollector;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
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

public class DynamoStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoStore.class);

  public static enum ValueType {
    REF(InternalRef.class, InternalRef.SCHEMA, false),
    L1(L1.class, com.dremio.nessie.versioned.impl.L1.SCHEMA),
    L2(L2.class, com.dremio.nessie.versioned.impl.L2.SCHEMA),
    L3(L3.class, com.dremio.nessie.versioned.impl.L3.SCHEMA),
    VALUE(InternalValue.class, InternalValue.SCHEMA),
    COMMIT_METADATA(InternalCommitMetadata.class, InternalCommitMetadata.SCHEMA);

    private final Class<?> objectClass;
    private final SimpleSchema<?> schema;
    private final boolean immutable;

    ValueType(Class<?> objectClass, SimpleSchema<?> schema) {
      this(objectClass, schema, true);
    }

    ValueType(Class<?> objectClass, SimpleSchema<?> schema, boolean immutable) {
      this.objectClass = objectClass;
      this.schema = schema;
      this.immutable = immutable;
    }

    public Class<?> getObjectClass() {
      return objectClass;
    }

    @SuppressWarnings("unchecked")
    public <T> SimpleSchema<T> getSchema() {
      return (SimpleSchema<T>) schema;
    }

    public boolean isImmutable() {
      return immutable;
    }
  }

  static final String KEY_NAME = "id";

  private final int paginationSize = 100;
  private final DynamoStoreConfig config;

  private DynamoDbClient client;
  private DynamoDbAsyncClient async;
  private final ImmutableMap<ValueType, String> tableNames;

  /**
   * create a DynamoStore.
   */
  public DynamoStore(DynamoStoreConfig config) {
    this.config = config;
    this.tableNames = ImmutableMap.<ValueType, String>builder()
        .put(ValueType.REF, config.getRefTableName())
        .put(ValueType.L1, config.getTreeTableName())
        .put(ValueType.L2, config.getTreeTableName())
        .put(ValueType.L3, config.getTreeTableName())
        .put(ValueType.VALUE, config.getValueTableName())
        .put(ValueType.COMMIT_METADATA, config.getMetadataTableName())
        .build();
  }

  /**
   * start the DynamoStore.
   */
  public void start() {
    DynamoDbClientBuilder b1 = DynamoDbClient.builder();
    b1.httpClient(ApacheHttpClient.create());
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

  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * Load the collection of loadsteps in order.
   *
   * <p>This Will fail if any load within any step. Consumers are informed as the
   * records are loaded so this load may leave inputs in a partial state.
   *
   * @param loadstep The step to load
   */
  void load(LoadStep loadstep) throws ReferenceNotFoundException {

    while (true) { // for each load step in the chain.
      List<ListMultimap<String, LoadOp<?>>> stepPages = paginateLoads(loadstep, paginationSize);

      for (ListMultimap<String, LoadOp<?>> l : stepPages) {
        Map<String, KeysAndAttributes> loads = l.keySet().stream().collect(Collectors.toMap(Function.identity(), table -> {
          List<LoadOp<?>> loadList = l.get(table);
          List<Map<String, AttributeValue>> keys = loadList.stream()
              .map(load -> ImmutableMap.of(KEY_NAME, load.getId().toAttributeValue()))
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
              throw new ReferenceNotFoundException("Unable to find requested ref.");
            }

            throw new DynamoGeneralReadFailure(
                String.format("[%d] object(s) missing in table read [%s]. \n\nObjects expected: %s\n\nObjects Received: %s",
                missingResponses, table, loadList, responses));
          }

          // unfortunately, responses don't come in the order of the requests so we need to map between ids.
          Map<Id, LoadOp<?>> opMap = loadList.stream().collect(Collectors.toMap(LoadOp::getId, Function.identity()));
          for (int i = 0; i < values.size(); i++) {
            Map<String, AttributeValue> item = values.get(i);
            opMap.get(Id.fromAttributeValue(item.get(KEY_NAME))).loaded(item);
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

  <V> boolean putIfAbsent(ValueType type, V value) {
    ConditionExpression condition = ConditionExpression.of(ExpressionFunction.attributeNotExists(ExpressionPath.builder(KEY_NAME).build()));
    try {
      put(type, value, Optional.of(condition));
      return true;
    } catch (ConditionalCheckFailedException ex) {
      return false;
    }
  }

  String tableName(ValueType valueType) {
    return tableNames.get(valueType);
  }

  @VisibleForTesting
  void deleteTables() {
    Arrays.stream(ValueType.values()).map(v -> tableNames.get(v)).collect(Collectors.toSet()).forEach(table -> {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(table).build());
      } catch (ResourceNotFoundException ex) {
        // ignore.
      }
    });

  }

  @SuppressWarnings("unchecked")
  <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    Preconditions.checkArgument(type.getObjectClass().isAssignableFrom(value.getClass()),
        "ValueType %s doesn't extend expected type %s.", value.getClass().getName(), type.getObjectClass().getName());
    Map<String, AttributeValue> attributes = ((SimpleSchema<V>)type.schema).itemToMap(value, true);

    PutItemRequest.Builder builder = PutItemRequest.builder()
        .tableName(tableNames.get(type))
        .item(attributes);

    if (conditionUnAliased.isPresent()) {
      AliasCollector c = new AliasCollector();
      ConditionExpression aliased = conditionUnAliased.get().alias(c);
      c.apply(builder).conditionExpression(aliased.toConditionExpressionString());
    }

    client.putItem(builder.build());
  }

  boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    DeleteItemRequest.Builder delete = DeleteItemRequest.builder()
        .key(id.toKeyMap())
        .tableName(tableNames.get(type));
    if (condition.isPresent()) {
      AliasCollector collector = new AliasCollector();
      ConditionExpression aliased = condition.get().alias(collector);
      collector.apply(delete);
      delete.conditionExpression(aliased.toConditionExpressionString());
    }

    try {
      client.deleteItem(delete.build());
      return true;
    } catch (ConditionalCheckFailedException ex) {
      LOGGER.debug("Failure during conditional check.", ex);
      return false;
    }
  }

  void save(List<SaveOp<?>> ops) {
    List<CompletableFuture<BatchWriteItemResponse>> saves =  new ArrayList<>();
    for (int i = 0; i < ops.size(); i += paginationSize) {

      ListMultimap<String, SaveOp<?>> mm =
          Multimaps.index(ops.subList(i, Math.min(i + paginationSize, ops.size())), l -> tableNames.get(l.getType()));
      ListMultimap<String, WriteRequest> writes = Multimaps.transformValues(mm, save -> {
        return WriteRequest.builder().putRequest(PutRequest.builder().item(save.toAttributeValues()).build()).build();
      });
      BatchWriteItemRequest batch = BatchWriteItemRequest.builder().requestItems(writes.asMap()).build();
      saves.add(async.batchWriteItem(batch));
    }

    try {
      CompletableFuture.allOf(saves.toArray(new CompletableFuture[0])).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwables.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  <V> V loadSingle(ValueType valueType, Id id) {
    GetItemResponse response = client.getItem(GetItemRequest.builder()
        .tableName(tableNames.get(valueType))
        .key(ImmutableMap.of(KEY_NAME, id.toAttributeValue()))
        .consistentRead(true)
        .build());
    if (!response.hasItem()) {
      throw ResourceNotFoundException.builder().message("Unable to load item.").build();
    }
    return (V) valueType.schema.mapToItem(response.item());
  }

  /**
   * Do a conditional update. If the condition succeeds, return the values in the object. If it fails, return a Optional.empty().
   *
   * @param <IN> The value type class that the UpdateExpression will be applied to.
   * @param <OUT> The value type class that the return expression is expected to be.
   * @param input The value type class that the UpdateExpression will be applied to.
   * @param update The update expression to use.
   * @param condition The optional condition to consider before applying the update.
   * @return The complete value if the update was successful, otherwise Optional.empty()
   * @throws ReferenceNotFoundException Thrown if the underlying id doesn't have an object.
   */
  @SuppressWarnings("unchecked")
  <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws ReferenceNotFoundException {
    try {
      AliasCollector collector = new AliasCollector();
      UpdateExpression aliased = update.alias(collector);
      Optional<ConditionExpression> aliasedCondition = condition.map(e -> e.alias(collector));
      UpdateItemRequest.Builder updateRequest = collector.apply(UpdateItemRequest.builder())
          .returnValues(ReturnValue.ALL_NEW)
          .tableName(tableNames.get(type))
          .key(ImmutableMap.of(KEY_NAME, id.toAttributeValue()))
          .updateExpression(aliased.toUpdateExpressionString());
      aliasedCondition.ifPresent(e -> updateRequest.conditionExpression(e.toConditionExpressionString()));
      UpdateItemRequest builtRequest = updateRequest.build();
      UpdateItemResponse response = client.updateItem(builtRequest);
      return (Optional<V>) Optional.of(type.schema.mapToItem(response.attributes()));
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("Unable to find value.", ex);
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

  Stream<InternalRef> getRefs() {
    return client.scanPaginator(ScanRequest.builder().tableName(tableNames.get(ValueType.REF)).build())
        .stream()
        .flatMap(r -> r.items().stream())
        .map(i -> ValueType.REF.<InternalRef>getSchema().mapToItem(i));
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
