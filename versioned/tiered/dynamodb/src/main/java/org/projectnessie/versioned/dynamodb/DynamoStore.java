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
package org.projectnessie.versioned.dynamodb;

import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.attributeValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.deserializeId;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.idValue;
import static org.projectnessie.versioned.dynamodb.DynamoBaseValue.ID;
import static org.projectnessie.versioned.dynamodb.DynamoSerDe.deserializeToConsumer;
import static org.projectnessie.versioned.dynamodb.DynamoSerDe.serializeWithConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.dynamodb.metrics.DynamoMetricsPublisher;
import org.projectnessie.versioned.dynamodb.metrics.TracingExecutionInterceptor;
import org.projectnessie.versioned.impl.EntityStoreHelper;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadOp;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.StoreOperationException;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.projectnessie.versioned.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import io.opentracing.util.GlobalTracer;
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
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
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

  /**
   * The <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html">Dynamo docs</a>
   * state a batch-write be rejected, if it contains more than 25 requests in a batch.
   */
  static final int DYNAMO_BATCH_WRITE_LIMIT_COUNT = 25;

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoStore.class);

  private final int paginationSize = LOAD_SIZE;
  private final DynamoStoreConfig config;

  private DynamoDbClient client;
  private DynamoDbAsyncClient async;
  private final ImmutableMap<ValueType<?>, String> tableNames;

  /**
   * create a DynamoStore.
   */
  public DynamoStore(DynamoStoreConfig config) {
    this.config = config;
    this.tableNames = ValueType.values().stream()
        .collect(ImmutableMap.toImmutableMap(v -> v, v -> v.getTableName(config.getTablePrefix())));

    if (tableNames.size() != new HashSet<>(tableNames.values()).size()) {
      throw new IllegalArgumentException("Each Nessie dynamo table must be named distinctly.");
    }
  }

  @Override
  public void start() {
    if (client != null && async != null) {
      return; // no-op
    }

    try {
      DynamoMetricsPublisher publisher = new DynamoMetricsPublisher();
      TracingExecutionInterceptor tracing = new TracingExecutionInterceptor(GlobalTracer.get());

      DynamoDbClientBuilder b1 = DynamoDbClient.builder();
      b1.httpClient(UrlConnectionHttpClient.create());
      b1.overrideConfiguration(
          x -> x.addExecutionInterceptor(publisher.interceptor()).addExecutionInterceptor(tracing).addMetricPublisher(publisher));

      DynamoDbAsyncClientBuilder b2 = DynamoDbAsyncClient.builder();
      b2.httpClient(NettyNioAsyncHttpClient.create());
      b2.overrideConfiguration(
          x -> x.addExecutionInterceptor(publisher.interceptor()).addExecutionInterceptor(tracing).addMetricPublisher(publisher));

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
        ValueType.values().stream()
          .map(tableNames::get)
          .collect(Collectors.toSet())
            .forEach(this::createIfMissing);

        // make sure we have an empty l1 (ignore result, doesn't matter)
        EntityStoreHelper.storeMinimumEntities(this::putIfAbsent);
      }
    } catch (Exception ex) {
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
    try {
      AutoCloseables.close(client, async);
      client = null;
      async = null;
    } catch (Exception e) {
      throw new StoreOperationException("Failed to close store", e);
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
              .map(load -> Collections.singletonMap(DynamoBaseValue.ID, idValue(load.getId())))
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
            ValueType<?> loadType = loadList.get(0).getValueType();
            if (loadType == ValueType.REF || loadType == ValueType.L1) {
              throw new NotFoundException("Unable to find requested ref.");
            }

            throw new NotFoundException(
                String.format("[%d] object(s) missing in table read [%s]. \n\nObjects expected: %s\n\nObjects Received: %s",
                missingResponses, table, loadList, responses));
          }

          // unfortunately, responses don't come in the order of the requests so we need to map between ids.
          Map<Id, LoadOp<?>> opMap = loadList.stream().collect(Collectors.toMap(LoadOp::getId, Function.identity()));
          for (Map<String, AttributeValue> item : values) {
            @SuppressWarnings("rawtypes") ValueType valueType = ValueType.byValueName(attributeValue(item, ValueType.SCHEMA_TYPE).s());
            Id id = deserializeId(item, ID);
            LoadOp<?> loadOp = opMap.get(id);
            if (loadOp == null) {
              throw new IllegalStateException("No load-op for loaded ID " + id);
            }

            deserializeToConsumer(valueType, item, loadOp.getReceiver());
            loadOp.done();
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
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    ConditionExpression condition = ConditionExpression.of(ExpressionFunction.attributeNotExists(ExpressionPath.builder(KEY_NAME).build()));
    try {
      put(saveOp, Optional.of(condition));
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
    ValueType.values().stream().map(tableNames::get).collect(Collectors.toSet()).forEach(table -> {
      try {
        client.deleteTable(DeleteTableRequest.builder().tableName(table).build());
      } catch (ResourceNotFoundException ex) {
        // ignore.
      }
    });

  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> conditionUnAliased) {
    Map<String, AttributeValue> attributes = serializeWithConsumer(saveOp);

    PutItemRequest.Builder builder = PutItemRequest.builder()
        .tableName(tableNames.get(saveOp.getType()))
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
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    DeleteItemRequest.Builder delete = DeleteItemRequest.builder()
        .key(Collections.singletonMap(DynamoBaseValue.ID, idValue(id)))
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

  private static ConditionExpression addTypeCheck(ValueType<?> type, Optional<ConditionExpression> possibleExpression) {
    final ExpressionFunction checkType = ExpressionFunction.equals(
        ExpressionPath.builder(ValueType.SCHEMA_TYPE).build(),
        Entity.ofString(type.getValueName()));
    return possibleExpression.map(ce -> ce.and(checkType)).orElse(ConditionExpression.of(checkType));
  }

  /**
   * Internal class used to collect batch-operation ({@code BATCH}) of type {@code REQ}, where
   * each batch-operation gets at max the given number of requests {{@code REQ}}, but in a map of
   * table-name-to-collection-of-write-requests.
   * <p>This class is very targeted to be used for DynamoDB batch-write requests, and is
   * therefore here as a "private" class. If it feels appropriate elsewhere, it can probably
   * be extended and maybe moved elsewhere.</p>
   *
   * @param <IN> Input object type - this is {@link SaveOp} for the {@link #save(List)} use-case
   * @param <KEY> Key parameter as required by the batch - this is the table-name as a {@link String}
   *             for DynamoDB batch-writes. The {@code keyMapper} function maps an {@code IN}
   *             to a {@code KEY}.
   * @param <REQ> Value parameter as required by the bach - this is a {@link WriteRequest} for
   *             DynamoDB batch-writes. The {@code valueMapper} function maps an {@code IN}
   *             to a {@code REQ}.
   * @param <BATCH> the resulting type as returned by the {@code emitter} function after submitting
   *               the batch-operation from the {@code Map<KEY, Collection<REQ>>}.
   */
  static class BatchesCollector<IN, KEY, REQ, BATCH> {
    private final Function<IN, KEY> keyMapper;
    private final Function<IN, REQ> valueMapper;
    private final Function<Map<KEY, Collection<REQ>>, BATCH> emitter;
    private final List<BATCH> result = new ArrayList<>();
    private final int maxBatchSize;

    private int requests;
    private final Map<KEY, Collection<REQ>> current = new HashMap<>();

    BatchesCollector(Function<IN, KEY> keyMapper, Function<IN, REQ> valueMapper,
        Function<Map<KEY, Collection<REQ>>, BATCH> emitter, int maxBatchSize) {
      Preconditions.checkArgument(maxBatchSize > 0);
      Preconditions.checkNotNull(keyMapper);
      Preconditions.checkNotNull(valueMapper);
      Preconditions.checkNotNull(emitter);
      this.keyMapper = keyMapper;
      this.valueMapper = valueMapper;
      this.emitter = emitter;
      this.maxBatchSize = maxBatchSize;
    }

    private void handle(IN op) {
      KEY key = keyMapper.apply(op);
      REQ req = valueMapper.apply(op);

      current.computeIfAbsent(key, k -> new ArrayList<>()).add(req);
      if (++requests == maxBatchSize) {
        emit();
      }
    }

    private void emit() {
      if (requests > 0) {
        result.add(emitter.apply(new HashMap<>(current)));
        requests = 0;
        current.clear();
      }
    }

    List<BATCH> collect(Stream<IN> input) {
      input.forEach(this::handle);
      emit();
      return result;
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    // max number of requests per batch-write
    int maxBatchSize = Math.min(DYNAMO_BATCH_WRITE_LIMIT_COUNT, paginationSize);

    // "Collector" for `BatchWriteItemRequest`s, with at `maxBatchSize` requests per batch-write
    BatchesCollector<SaveOp<?>, String, WriteRequest, CompletableFuture<BatchWriteItemResponse>> saveBatch =
        new BatchesCollector<>(
            op -> tableNames.get(op.getType()),
            op -> WriteRequest.builder().putRequest(PutRequest.builder().item(serializeWithConsumer(op)).build()).build(),
            grouped -> async.batchWriteItem(BatchWriteItemRequest.builder().requestItems(grouped).build()),
            maxBatchSize);

    // Collect the already fired `BatchWriteItemRequest`
    List<CompletableFuture<BatchWriteItemResponse>> saves = saveBatch.collect(ops.stream());

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

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> valueType, Id id, C consumer) {
    GetItemResponse response = client.getItem(GetItemRequest.builder()
        .tableName(tableNames.get(valueType))
        .key(Collections.singletonMap(DynamoBaseValue.ID, idValue(id)))
        .consistentRead(true)
        .build());
    if (!response.hasItem()) {
      throw new NotFoundException("Unable to load item.");
    }
    deserializeToConsumer(valueType, response.item(), consumer);
  }

  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer) throws NotFoundException {
    try {
      AliasCollectorImpl collector = new AliasCollectorImpl();
      UpdateExpression aliased = update.alias(collector);
      Optional<ConditionExpression> aliasedCondition = condition.map(e -> e.alias(collector));
      UpdateItemRequest.Builder updateRequest = collector.apply(UpdateItemRequest.builder())
          .returnValues(ReturnValue.ALL_NEW)
          .tableName(tableNames.get(type))
          .key(Collections.singletonMap(DynamoBaseValue.ID, idValue(id)))
          .updateExpression(aliased.toUpdateExpressionString());
      aliasedCondition.ifPresent(e -> updateRequest.conditionExpression(e.toConditionExpressionString()));
      UpdateItemRequest builtRequest = updateRequest.build();
      UpdateItemResponse response = client.updateItem(builtRequest);
      consumer.ifPresent(c -> deserializeToConsumer(type, response.attributes(), c));
      return true;
    } catch (ResourceNotFoundException ex) {
      throw new NotFoundException("Unable to find value.", ex);
    } catch (ConditionalCheckFailedException checkFailed) {
      LOGGER.debug("Conditional check failed.", checkFailed);
      return false;
    }
  }

  private boolean tableExists(String name) {
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
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    return client.scanPaginator(ScanRequest.builder().tableName(tableNames.get(type)).build())
        .stream()
        .flatMap(r -> r.items().stream())
        .map(i -> consumer -> deserializeToConsumer(type, i, consumer));
  }

  private void createIfMissing(String name) {
    if (!tableExists(name)) {
      createTable(name);
    }
  }

  private void createTable(String name) {
    client.createTable(CreateTableRequest.builder()
        .tableName(name)
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName(KEY_NAME)
            .attributeType(ScalarAttributeType.B)
            .build())
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .keySchema(KeySchemaElement.builder()
            .attributeName(KEY_NAME)
            .keyType(KeyType.HASH)
            .build())
        .build());
  }

  private static void verifyKeySchema(TableDescription description) {
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
