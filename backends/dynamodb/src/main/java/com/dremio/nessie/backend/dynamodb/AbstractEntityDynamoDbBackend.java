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

package com.dremio.nessie.backend.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.backend.VersionedWrapper;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

@Deprecated
abstract class AbstractEntityDynamoDbBackend<M> implements EntityBackend<M> {

  private final DynamoDbClient client;
  private final String tableName;
  private final boolean versioned;
  private final Tracer tracer = GlobalTracer.get();
  private final Counter deleteCounter;
  private final Counter putCounter;
  private final Counter getCounter;
  private final Counter versionedCounter;
  private final DynamoMetrics getAllMetrics;
  private final DynamoMetrics getMetrics;
  private final DynamoMetrics putAllMetrics;
  private final DynamoMetrics putMetrics;
  private final DynamoMetrics deleteMetrics;

  public AbstractEntityDynamoDbBackend(DynamoDbClient client,
                                       String tableName,
                                       boolean versioned) {
    this.client = client;
    this.tableName = tableName;
    this.versioned = versioned;
    deleteCounter = Metrics.counter("dynamo", "capacity", "delete");
    putCounter = Metrics.counter("dynamo", "capacity", "put");
    getCounter = Metrics.counter("dynamo", "capacity", "get");
    versionedCounter = Metrics.counter("dynamo", "versioned", "count");
    getAllMetrics = new DynamoMetrics("get-all");
    getMetrics = new DynamoMetrics("get");
    putAllMetrics = new DynamoMetrics("put-all");
    putMetrics = new DynamoMetrics("put");
    deleteMetrics = new DynamoMetrics("delete");
  }

  protected abstract Map<String, AttributeValue> toDynamoDB(VersionedWrapper<M> from);

  protected abstract VersionedWrapper<M> fromDynamoDB(Map<String, AttributeValue> from);

  @Override
  public VersionedWrapper<M> get(String name) {
    Span span = tracer.buildSpan("dynamo-get").start();
    try (Scope scope = tracer.scopeManager().activate(span, true);
         AutoCloseable mc = getMetrics.start()) {
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("uuid", AttributeValue.builder().s(name).build());
      GetItemRequest request = GetItemRequest.builder()
                                             .consistentRead(true)
                                             .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                             .key(key)
                                             .tableName(tableName)
                                             .build();
      GetItemResponse response = client.getItem(request);
      getCounter.increment((long) Math.ceil(response.consumedCapacity().capacityUnits()));
      Map<String, AttributeValue> obj = response.item();

      if (obj == null || obj.isEmpty()) {
        return null;
      }
      return fromDynamoDB(obj);
    } catch (Exception e) {
      // can't happen
      throw new RuntimeException(e);
    }
  }

  public List<VersionedWrapper<M>> getAll(boolean includeDeleted) {
    Span span = tracer.buildSpan("dynamo-get-all").start();
    try (Scope scope = tracer.scopeManager().activate(span, true);
         AutoCloseable mc = getAllMetrics.start()) {
      ScanRequest request = ScanRequest.builder()
                                       .tableName(tableName)
                                       .consistentRead(true)
                                       .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                       .build();

      ScanIterable results = client.scanPaginator(request);
      List<VersionedWrapper<M>> resultList = new ArrayList<>();
      results.iterator().forEachRemaining(response -> {
        response.items().stream().map(this::fromDynamoDB).forEach(resultList::add);
        getCounter.increment((long) Math.ceil(response.consumedCapacity().capacityUnits()));
      });
      return resultList;
    } catch (Exception e) {
      // can't happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public VersionedWrapper<M> update(String name, VersionedWrapper<M> obj) {
    Span span = tracer.buildSpan("dynamo-put").start();
    try (Scope scope = tracer.scopeManager().activate(span, true);
         AutoCloseable mc = putMetrics.start()) {
      Map<String, AttributeValue> item = toDynamoDB(obj);
      Builder builder = PutItemRequest.builder()
                                      .tableName(tableName);
      if (versioned) {
        versionedCounter.increment();
        String expression;
        Long version = obj.getVersion();
        if (version == null) {
          item.put("version", AttributeValue.builder().n("1").build());
          expression = "attribute_not_exists(version)";
        } else {
          String newVersion = Long.toString(version + 1);
          item.put("version", AttributeValue.builder().n(newVersion).build());
          expression = "version = :v";
          Map<String, AttributeValue> expressionVals = new HashMap<>();
          expressionVals.put(":v", AttributeValue.builder().n(version.toString()).build());
          builder.expressionAttributeValues(expressionVals);
        }
        builder.conditionExpression(expression);
      }

      PutItemResponse response = client.putItem(builder.item(item)
                                                       .returnConsumedCapacity(
                                                         ReturnConsumedCapacity.TOTAL)
                                                       .build());
      putCounter.increment((long) Math.ceil(response.consumedCapacity().capacityUnits()));
      return get(name);
    } catch (Exception e) {
      // can't happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateAll(Map<String, VersionedWrapper<M>> transaction) {
    Span span = tracer.buildSpan("dynamo-put-all").start();
    try (Scope scope = tracer.scopeManager().activate(span, true);
         AutoCloseable mc = putAllMetrics.start()) {
      Map<String, List<WriteRequest>> items = new HashMap<>();
      List<WriteRequest> writeRequests =
          transaction.values()
                     .stream()
                     .map(x -> PutRequest.builder().item(toDynamoDB(x)).build())
                     .map(x -> WriteRequest.builder().putRequest(x).build())
                     .collect(Collectors.toList());
      items.put(tableName, writeRequests);
      BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                                                           .requestItems(items)
                                                           .returnConsumedCapacity(
                                                             ReturnConsumedCapacity.TOTAL)
                                                           .build();
      BatchWriteItemResponse response = client.batchWriteItem(request);
      putCounter.increment((long) Math.ceil(response.consumedCapacity()
                                              .stream()
                                              .mapToDouble(ConsumedCapacity::capacityUnits)
                                              .sum()));
    } catch (Exception e) {
      // can't happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(String name) {
    Span span = tracer.buildSpan("dynamo-remove").start();
    try (Scope scope = tracer.scopeManager().activate(span, true);
         AutoCloseable mc = deleteMetrics.start()) {
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("uuid", AttributeValue.builder().s(name).build());
      DeleteItemRequest request = DeleteItemRequest.builder()
                                                   .tableName(tableName)
                                                   .key(key)
                                                   .returnConsumedCapacity(
                                                     ReturnConsumedCapacity.TOTAL)
                                                   .build();
      DeleteItemResponse response = client.deleteItem(request);
      deleteCounter.increment((long) Math.ceil(response.consumedCapacity().capacityUnits()));
    } catch (Exception e) {
      // can't happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private static class DynamoMetrics {
    private final Counter counter;
    private final Timer timer;

    private DynamoMetrics(String name) {
      timer = Timer.builder("dynamodb-function")
                   .tag("timer", name)
                   //.publishPercentiles(0.5, 0.95) // median and 95th percentile
                   .publishPercentileHistogram()
                   //.sla(Duration.ofMillis(100))
                   //.minimumExpectedValue(Duration.ofMillis(1))
                   //.maximumExpectedValue(Duration.ofSeconds(10))
                   .register(Metrics.globalRegistry);
      counter = Metrics.counter("dynamodb-function", "counter", name);
    }

    AutoCloseable start() {
      long start = System.nanoTime();
      return () -> {
        counter.increment();
        timer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      };
    }
  }
}
