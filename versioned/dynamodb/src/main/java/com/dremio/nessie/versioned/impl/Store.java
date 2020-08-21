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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
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
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

class Store implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);

  public static enum ValueType {
    REF(REF_TABLE_NAME, InternalRef.class, InternalRef.SCHEMA),
    L1(OBJ_TABLE_NAME, L1.class, com.dremio.nessie.versioned.impl.L1.SCHEMA),
    L2(OBJ_TABLE_NAME, L2.class, com.dremio.nessie.versioned.impl.L2.SCHEMA),
    L3(OBJ_TABLE_NAME, L3.class, com.dremio.nessie.versioned.impl.L3.SCHEMA),
    VALUE(OBJ_TABLE_NAME, WrappedValueBean.class, WrappedValueBean.SCHEMA),
    COMMIT_METADATA(OBJ_TABLE_NAME, WrappedValueBean.class, WrappedValueBean.SCHEMA);

    private final String table;
    private final Class<?> objectClass;
    private final TableSchema<?> schema;
    ValueType(String table, Class<?> objectClass, TableSchema<?> schema) {
      this.table = table;
      this.objectClass = objectClass;
      this.schema = schema;
    }

    public Class<?> getObjectClass() {
      return objectClass;
    }

    public String getTableName() {
      return table;
    }

    @SuppressWarnings("unchecked")
    public <T> TableSchema<T> getSchema() {
      return (TableSchema<T>) schema;
    }
  }

  static final String KEY_NAME = "id";

  private static final String REF_TABLE_NAME = "names";
  private static final String OBJ_TABLE_NAME = "objects";
  private static final String COMMIT_TABLE_NAME = "commits";

  private final int getPaginationSize = 25;
  private DynamoDbTable<L1> l1Table;
  private DynamoDbTable<L2> l2Table;
  private DynamoDbTable<L3> l3Table;
  private DynamoDbTable<InternalRef> refTable;
  private DynamoDbTable<WrappedValueBean> valueTable;
  private DynamoDbTable<WrappedValueBean> commitTable;
  private Map<ValueType, DynamoDbTable<?>> typeToTable;

  private DynamoDbClient client;
  private DynamoDbAsyncClient async;
  private DynamoDbEnhancedClient enh;

  public Store() {
  }

  public void start() throws URISyntaxException {
    URI local = new URI("http://localhost:8000");
    client = DynamoDbClient.builder().endpointOverride(local).region(Region.US_WEST_2).build();
    async = DynamoDbAsyncClient.builder().endpointOverride(local).region(Region.US_WEST_2).build();
    enh = DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();

    Arrays.stream(ValueType.values())
      .map(ValueType::getTableName)
      .collect(Collectors.toSet())
      .forEach(table -> createIfMissing(table));

    refTable = enh.table(REF_TABLE_NAME, InternalRef.SCHEMA);
    l1Table = enh.table(OBJ_TABLE_NAME, L1.SCHEMA);
    l2Table = enh.table(OBJ_TABLE_NAME, L2.SCHEMA);
    l3Table = enh.table(OBJ_TABLE_NAME, L3.SCHEMA);
    valueTable = enh.table(OBJ_TABLE_NAME, WrappedValueBean.SCHEMA);
    commitTable = enh.table(COMMIT_TABLE_NAME, WrappedValueBean.SCHEMA);

    typeToTable = ImmutableMap.<ValueType, DynamoDbTable<?>>builder()
        .put(ValueType.REF, refTable)
        .put(ValueType.L1, l1Table)
        .put(ValueType.L2, l2Table)
        .put(ValueType.L3, l3Table)
        .put(ValueType.VALUE, valueTable)
        .put(ValueType.COMMIT_METADATA, commitTable)
        .build();

    // make sure we have an empty l1 (ignore result, doesn't matter)
    putIfAbsent(ValueType.L1, L1.EMPTY);
    putIfAbsent(ValueType.L2, L2.EMPTY);
    putIfAbsent(ValueType.L3, L3.EMPTY);
  }

  public void close() {
    client.close();
  }

  /**
   * Load the collection of loadsteps in order.
   *
   * This Will fail if any load within any step. Consumers are informed as the
   * records are loaded so this load may leave inputs in a partial state.
   *
   * @param loadstep
   */
  void load(LoadStep loadstep) {

    while (true) { // for each load step in the chain.
      System.out.println("Start load step.");
      List<ListMultimap<String, LoadOp<?>>> stepPages = loadstep.paginateLoads(getPaginationSize);

      stepPages.forEach(l -> {
        Map<String, KeysAndAttributes> loads = l.keySet().stream().collect(Collectors.toMap(table -> table, table -> {
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
        responses.keySet().forEach(table -> {
          List<LoadOp<?>> loadList = l.get(table);
          List<Map<String, AttributeValue>> values = responses.get(table);
          int missingResponses = loadList.size() - values.size();
          Preconditions.checkArgument(missingResponses == 0, "[%s] object(s) missing in table read [%s]. \n\nObjects expected: %s\n\nObjects Received: %s", missingResponses, table, loadList, responses);
          System.out.println("Loads for table: " + table + ", total of " + loadList.size());
          for(int i =0; i < values.size(); i++) {
            System.out.println("[LOAD] " + loadList.get(i) + " loaded with " + values.get(i));
            loadList.get(i).loaded(values.get(i));
          }
        });
      });
      Optional<LoadStep> next = loadstep.getNext();

      if(!next.isPresent()) {
        break;
      }
      loadstep = next.get();
    }
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

  @SuppressWarnings("unchecked")
  <V> void put(ValueType type, V value) {
    put(type, value, Optional.empty());
  }

  @SuppressWarnings("unchecked")
  <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    Preconditions.checkArgument(type.getObjectClass().equals(value.getClass()), "ValueType %s doesn't match expected type %s.", value.getClass().getName(), type.getObjectClass().getName());
    Map<String, AttributeValue> attributes = ((TableSchema<V>)type.schema).itemToMap(value, true);

    PutItemRequest.Builder builder = PutItemRequest.builder()
      .tableName(type.getTableName())
      .item(attributes);

    if(conditionUnAliased.isPresent()) {
      AliasCollector c = new AliasCollector();
      ConditionExpression aliased = conditionUnAliased.get().alias(c);
      c.apply(builder).conditionExpression(aliased.toConditionExpressionString());
    }

    client.putItem(builder.build());
  }

  boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    DeleteItemRequest.Builder delete = DeleteItemRequest.builder()
        .key(id.toKeyMap())
        .tableName(type.getTableName());
    if(condition.isPresent()) {
      AliasCollector collector = new AliasCollector();
      ConditionExpression aliased = condition.get().alias(collector);
      collector.apply(delete);
      delete.conditionExpression(aliased.toConditionExpressionString());
    }

    try {
      client.deleteItem(delete.build());
      return true;
    } catch(ConditionalCheckFailedException ex) {
      LOGGER.debug("Failure during conditional check.", ex);
      return false;
    }
  }

  void save(List<SaveOp<?>> ops) {
    List<CompletableFuture<BatchWriteItemResponse>> saves =  new ArrayList<>();
    for(int i = 0; i < ops.size(); i += getPaginationSize) {

      ListMultimap<String, SaveOp<?>> mm = Multimaps.index(ops.subList(i, Math.min(i + getPaginationSize, ops.size())), l -> l.getType().getTableName());
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
      throw new RuntimeException(e.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  <V> V loadSingle(ValueType valueType, Id id) {
    GetItemResponse response = client.getItem(GetItemRequest.builder().tableName(valueType.getTableName()).key(ImmutableMap.of(KEY_NAME, id.toAttributeValue())).consistentRead(true).build());
    System.out.println(response);
    return (V) valueType.schema.mapToItem(response.item());
  }

  /**
   * Do a conditional update. If the condition succeeds, return the values in the object. If it fails, return a Optional.empty().
   * @param request
   * @return
   * @throws ReferenceNotFoundException
   */

  /**
   * Do a conditional update. If the condition succeeds, return the values in the object. If it fails, return a Optional.empty().
   *
   * @param <IN> The value type class that the UpdateExpression will be applied to.
   * @param <OUT> The value type class that the return expression is expected to be.
   * @param input The value type class that the UpdateExpression will be applied to.
   * @param update
   * @param condition
   * @param output
   * @return
   * @throws ReferenceNotFoundException
   */
  @SuppressWarnings("unchecked")
  <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition) throws ReferenceNotFoundException {
    try {
      AliasCollector collector = new AliasCollector();
      UpdateExpression aliased = update.alias(collector);
      Optional<ConditionExpression> aliasedCondition = condition.map(e -> e.alias(collector));
      UpdateItemRequest.Builder updateRequest = collector.apply(UpdateItemRequest.builder())
          .returnValues(ReturnValue.ALL_NEW)
          .tableName(type.getTableName())
          .key(ImmutableMap.of(KEY_NAME, id.toAttributeValue()))
          .updateExpression(aliased.toUpdateExpressionString());
      aliasedCondition.ifPresent(e -> updateRequest.conditionExpression(e.toConditionExpressionString()));
      UpdateItemRequest builtRequest = updateRequest.build();
      System.out.println(client.getItem(GetItemRequest.builder().tableName(type.getTableName()).key(ImmutableMap.of(KEY_NAME, id.toAttributeValue())).build()));
      System.out.println(builtRequest);
      UpdateItemResponse response = client.updateItem(builtRequest);
      return (Optional<V>) Optional.of(type.schema.mapToItem(response.attributes()));
    } catch (ResourceNotFoundException ex) {
      throw new ReferenceNotFoundException("Unable to find value.", ex);
    } catch(ConditionalCheckFailedException checkFailed) {
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
    return refTable.scan().items().stream();
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
    if(elements.size() == 1) {
      KeySchemaElement key = elements.get(0);
      if(key.attributeName().equals(KEY_NAME)) {
        if(key.keyType() == KeyType.HASH) {
          return;
        }
      }
      throw new IllegalStateException(String.format("Invalid key schema for table: %s. Key schema should be a hash partitioned attribute with the name 'id'.", description.tableName()));
    }
  }
}
