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
package com.dremio.nessie.versioned.store.dynamo.metrics;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest;
import software.amazon.awssdk.core.interceptor.Context.ModifyRequest;
import software.amazon.awssdk.core.interceptor.Context.ModifyResponse;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Collect important metrics from dynamo. Both standard SDK metrics and consumed capacity for each request.
 *
 * <p>todo set common tags (eg region, stack etc)
 * todo add a filter on level https://micrometer.io/docs/concepts#_naming_meters
 */
public class DynamoMetricsPublisher implements MetricPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoMetricsPublisher.class);
  private static final Set<Class<?>> LOG_CACHE = new HashSet<>();
  private static final Map<String, Timer> TIMER_CACHE = new ConcurrentHashMap<>();
  private static final Map<String, AtomicInteger> GAUGE_CACHE = new ConcurrentHashMap<>();
  private static final Map<String, DistributionSummary> COUNTER_CACHE = new ConcurrentHashMap<>();
  private static final String UNKNOWN = "unknown";

  private static final Map<Class<? extends SdkResponse>, ToDoubleFunction<SdkResponse>> CONSUMED_CAPACITY;
  private static final Map<Class<? extends SdkResponse>, ToLongFunction<SdkResponse>> ITEM_COUNT_RESPONSE;
  private static final Map<Class<? extends SdkRequest>, ToLongFunction<SdkRequest>> ITEM_COUNT_REQUEST;
  private static final Map<Class<? extends SdkRequest>, Function<SdkRequest, SdkRequest>> ADD_RETURN_CAPACITY;


  static {
    CONSUMED_CAPACITY = ImmutableMap.<Class<? extends SdkResponse>, ToDoubleFunction<SdkResponse>>builder()
      .put(PutItemResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((PutItemResponse) r).consumedCapacity()))
      .put(BatchWriteItemResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((BatchWriteItemResponse) r).consumedCapacity()))
      .put(GetItemResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((GetItemResponse) r).consumedCapacity()))
      .put(BatchGetItemResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((BatchGetItemResponse) r).consumedCapacity()))
      .put(DeleteItemResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((DeleteItemResponse) r).consumedCapacity()))
      .put(ScanResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((ScanResponse) r).consumedCapacity()))
      .put(UpdateItemResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((UpdateItemResponse) r).consumedCapacity()))
      .put(QueryResponse.class,
        r -> DynamoMetricsPublisher.getConsumedCapacity(((QueryResponse) r).consumedCapacity()))
      .build();
    ADD_RETURN_CAPACITY = ImmutableMap.<Class<? extends SdkRequest>, Function<SdkRequest, SdkRequest>>builder()
      .put(PutItemRequest.class,
        r -> ((PutItemRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(BatchWriteItemRequest.class,
        r -> ((BatchWriteItemRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(GetItemRequest.class,
        r -> ((GetItemRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(BatchGetItemRequest.class,
        r -> ((BatchGetItemRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(DeleteItemRequest.class,
        r -> ((DeleteItemRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(ScanRequest.class,
        r -> ((ScanRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(UpdateItemRequest.class,
        r -> ((UpdateItemRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .put(QueryRequest.class,
        r -> ((QueryRequest) r).toBuilder().returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
      .build();
    ITEM_COUNT_REQUEST = ImmutableMap.<Class<? extends SdkRequest>, ToLongFunction<SdkRequest>>builder()
      .put(PutItemRequest.class, r -> ((PutItemRequest) r).hasItem() ? 1 : 0)
      .put(BatchWriteItemRequest.class,
        r -> ((BatchWriteItemRequest) r).requestItems().values().stream().map(List::size).mapToInt(x -> x).count())
      .put(DeleteItemRequest.class, r -> ((DeleteItemRequest) r).hasKey() ? 1 : 0)
      .put(UpdateItemRequest.class, r -> ((UpdateItemRequest) r).hasKey() ? 1 : 0)
      .build();
    ITEM_COUNT_RESPONSE = ImmutableMap.<Class<? extends SdkResponse>, ToLongFunction<SdkResponse>>builder()
      .put(GetItemResponse.class, r -> ((GetItemResponse) r).hasItem() ? 1 : 0)
      .put(BatchGetItemResponse.class,
        r -> ((BatchGetItemResponse) r).responses()
          .entrySet()
          .stream()
          .flatMap(x -> x.getValue().stream())
          .map(Map::size)
          .mapToInt(x -> x).count())
      .put(ScanResponse.class, r -> ((ScanResponse) r).count())
      .put(QueryResponse.class, r -> ((QueryResponse) r).count())
      .build();
  }

  public DynamoMetricsPublisher() {
  }

  @Override
  public void publish(MetricCollection metricCollection) {
    String operationName = UNKNOWN;
    String serviceId = UNKNOWN;
    for (MetricRecord<?> metricRecord : metricCollection) {
      String name = metricRecord.metric().name();
      if (name.equals(CoreMetric.OPERATION_NAME.name())) {
        operationName = metricRecord.value().toString();
      } else if (name.equals(CoreMetric.SERVICE_ID.name())) {
        serviceId = metricRecord.value().toString();
      }
    }
    publishInternal(metricCollection, operationName, serviceId);
  }

  private void publishInternal(MetricCollection metricCollection, String operationName, String serviceId) {
    metricCollection.stream().forEach(m -> {
      String name = m.metric().name();
      Object value = m.value();
      String metricName = String.format("%s.%s", serviceId, name);
      String level = m.metric().level().name();
      String key = String.format("%s/%s/%s", metricName, level, operationName);
      Tags tags = Tags.of("level", level, "operation", operationName);
      Class<?> valueClass = m.metric().valueClass();
      if (Duration.class == valueClass) {
        Timer timer = TIMER_CACHE.computeIfAbsent(key, n -> getTimer(metricName, tags));
        Duration duration = (Duration) value;
        timer.record(duration.toNanos(), TimeUnit.NANOSECONDS);
      } else if (Integer.class == valueClass) {
        measure(key, metricName, tags, (Integer) value);
      } else if (Boolean.class == valueClass) {
        measure(key, metricName, tags, ((Boolean) value) ? 1 : 0);
      }
    });
    metricCollection.children().forEach(m -> publishInternal(m, operationName, serviceId));
  }

  private static DistributionSummary getSummary(String name,Tags tags) {
    return Metrics.summary(name, tags);
  }

  private static AtomicInteger getGauge(String name, Tags tags) {
    return Metrics.gauge(name, Tags.of(tags), new AtomicInteger(0));
  }

  private static Timer getTimer(String name, Tags tags) {
    return Timer.builder(name)
                .tags(tags)
                .publishPercentileHistogram()
                .register(Metrics.globalRegistry);
  }

  @Override
  public void close() {

  }

  private static double getConsumedCapacity(ConsumedCapacity capacity) {
    return Optional.ofNullable(capacity).map(ConsumedCapacity::capacityUnits).orElse((double) 0);
  }

  private static double getConsumedCapacity(List<ConsumedCapacity> capacity) {
    return Optional.ofNullable(capacity).map(r -> r.stream().map(ConsumedCapacity::capacityUnits)
                                                   .mapToDouble(Double::doubleValue).sum()).orElse((double) 0);
  }

  private void measure(String key, String metricName, Tags tags, int consumedValue) {
    AtomicInteger integer = GAUGE_CACHE.computeIfAbsent(key, x -> getGauge(metricName + ".last", tags));
    DistributionSummary counter = COUNTER_CACHE.computeIfAbsent(key, x -> getSummary(metricName + ".summary", tags));
    counter.record(consumedValue);
    integer.set(consumedValue);
  }

  /**
   * Create execution interceptor which collects consumed capacity metrics.
   */
  public ExecutionInterceptor interceptor() {

    return new ExecutionInterceptor() {

      @Override
      public SdkRequest modifyRequest(ModifyRequest context,
                                      ExecutionAttributes executionAttributes) {
        SdkRequest request = context.request();
        return ADD_RETURN_CAPACITY.getOrDefault(request.getClass(), r -> r).apply(request);
      }

      @Override
      public SdkHttpRequest modifyHttpRequest(
          ModifyHttpRequest context,
          ExecutionAttributes executionAttributes) {
        SdkRequest request = context.request();
        String name = request.getClass().getSimpleName().replace("Request", "");
        ToLongFunction<SdkRequest> count = ITEM_COUNT_REQUEST.getOrDefault(request.getClass(), r -> {
          if (LOG_CACHE.add(r.getClass())) {
            LOG.info("Could not count number of entries for request of type {}", r.getClass());
          }
          return -1;
        });
        if (count != null) {
          String metricName = "DynamoDb.RecordCount";
          String key = String.format("%s/%s", metricName, name);
          Tags tags = Tags.of("operation", name);
          measure(key, metricName, tags, (int) count.applyAsLong(request));
        }
        return context.httpRequest();
      }

      @Override
      public SdkResponse modifyResponse(ModifyResponse context,
                                        ExecutionAttributes executionAttributes) {

        SdkResponse response = context.response();
        String name = response.getClass().getSimpleName().replace("Response", "");
        Tags tags = Tags.of("operation", name);

        ToLongFunction<SdkResponse> count = ITEM_COUNT_RESPONSE.getOrDefault(response.getClass(), r -> {
          if (LOG_CACHE.add(r.getClass())) {
            LOG.info("Could not count number of entries for response of type {}", r.getClass());
          }
          return -1;
        });
        ToDoubleFunction<SdkResponse> consumedValue = CONSUMED_CAPACITY.getOrDefault(response.getClass(), r -> {
          if (LOG_CACHE.add(r.getClass())) {
            LOG.info("Could not get returned used capacity for response of type {}", r.getClass());
          }
          return -1;
        });
        if (consumedValue != null) {
          String metricName = "DynamoDb.ConsumedCapacity";
          String key = String.format("%s/%s", metricName, name);
          measure(key, metricName, tags, (int) consumedValue.applyAsDouble(response));
        }
        if (count != null) {
          String metricName = "DynamoDb.RecordCount";
          String key = String.format("%s/%s", metricName, name);
          measure(key, metricName, tags, (int) count.applyAsLong(response));
        }
        return response;
      }
    };
  }
}
