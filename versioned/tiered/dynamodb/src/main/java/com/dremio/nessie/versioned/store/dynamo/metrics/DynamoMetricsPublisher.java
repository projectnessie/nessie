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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.interceptor.Context.ModifyHttpRequest;
import software.amazon.awssdk.core.interceptor.Context.ModifyResponse;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
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
  private static final Map<String, Timer> TIMER_CACHE = new HashMap<>();
  private static final Map<String, AtomicInteger> GAUGE_CACHE = new HashMap<>();
  private static final Map<String, DistributionSummary> COUNTER_CACHE = new HashMap<>();

  @Override
  public void publish(MetricCollection metricCollection) {
    AtomicReference<String> operationName = new AtomicReference<>("unknown");
    AtomicReference<String> serviceId = new AtomicReference<>("unknown");
    metricCollection.stream().forEach(r -> {
      String name = r.metric().name();
      if (name.equals("OperationName")) {
        operationName.set(r.value().toString());
      } else if (name.equals("ServiceId")) {
        serviceId.set(r.value().toString());
      }
    });
    publishInternal(metricCollection, operationName.get(), serviceId.get());
  }

  private void publishInternal(MetricCollection metricCollection, String operationName, String serviceId) {
    metricCollection.stream().forEach(m -> {
      String name = m.metric().name();
      Object value = m.value();
      String metricName = String.format("%s.%s", serviceId, name);
      String level = m.metric().level().name();
      String key = String.format("%s/%s/%s", metricName, level, operationName);
      Tags tags = Tags.of("level", level, "operation", operationName);
      if (Duration.class.equals(m.metric().valueClass())) {
        Timer timer = TIMER_CACHE.computeIfAbsent(key, n -> getTimer(metricName, tags));
        Duration duration = (Duration) value;
        timer.record(duration.toNanos(), TimeUnit.NANOSECONDS);
      } else if (Integer.class.equals(m.metric().valueClass()) || Boolean.class.equals(m.metric().valueClass())) {
        Integer intValue;
        if (Integer.class.equals(m.metric().valueClass())) {
          intValue = (Integer) value;
        } else {
          Boolean boolValue = (Boolean) value;
          intValue = boolValue ? 1 : 0;
        }
        measure(key, metricName, tags, (int) intValue);
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
                //.publishPercentiles(0.5, 0.95) // median and 95th percentile
                .publishPercentileHistogram()
                //.sla(Duration.ofMillis(100))
                //.minimumExpectedValue(Duration.ofMillis(1))
                //.maximumExpectedValue(Duration.ofSeconds(10))
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

  private final void measure(String key, String metricName, Tags tags, int consumedValue) {
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
      public SdkHttpRequest modifyHttpRequest(
          ModifyHttpRequest context,
          ExecutionAttributes executionAttributes) {
        SdkRequest request = context.request();
        long count;
        String name = request.getClass().getSimpleName().replace("Request", "");
        if (request instanceof PutItemRequest) {
          count = ((PutItemRequest) request).hasItem() ? 1 : 0;
        } else if (request instanceof BatchWriteItemRequest) {
          count = ((BatchWriteItemRequest) request).requestItems().values().stream().map(List::size).mapToInt(x -> x).count();
        } else if (request instanceof DeleteItemRequest) {
          count = ((DeleteItemRequest) request).hasKey() ? 1 : 0;
        } else if (request instanceof UpdateItemRequest) {
          count = ((UpdateItemRequest) request).hasKey() ? 1 : 0;
        } else {
          count = 0;
          name = null;
        }
        if (name != null) {
          String metricName = "DynamoDb.RecordCount";
          String key = String.format("%s/%s", metricName, name);
          Tags tags = Tags.of("operation", name);
          measure(key, metricName, tags, (int) count);
        }
        return context.httpRequest();
      }

      @Override
      public SdkResponse modifyResponse(ModifyResponse context,
                                        ExecutionAttributes executionAttributes) {
        double consumedValue;
        long count = -1;
        SdkResponse response = context.response();
        String name = response.getClass().getSimpleName().replace("Response", "");
        Tags tags = Tags.of("operation", name);
        if (response instanceof PutItemResponse) {
          ConsumedCapacity consumedCapacity = ((PutItemResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
        } else if (response instanceof BatchWriteItemResponse) {
          List<ConsumedCapacity> consumedCapacity = ((BatchWriteItemResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
        } else if (response instanceof GetItemResponse) {
          ConsumedCapacity consumedCapacity = ((GetItemResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
          count = ((GetItemResponse) response).hasItem() ? 1 : 0;
        } else if (response instanceof BatchGetItemResponse) {
          List<ConsumedCapacity> consumedCapacity = ((BatchGetItemResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
          count = ((BatchGetItemResponse) response).responses()
                                                   .entrySet()
                                                   .stream()
                                                   .flatMap(x -> x.getValue().stream())
                                                   .map(Map::size)
                                                   .mapToInt(x -> x).count();
        } else if (response instanceof DeleteItemResponse) {
          ConsumedCapacity consumedCapacity = ((DeleteItemResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
        } else if (response instanceof ScanResponse) {
          ConsumedCapacity consumedCapacity = ((ScanResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
          count = ((ScanResponse) response).count();
        } else if (response instanceof UpdateItemResponse) {
          ConsumedCapacity consumedCapacity = ((UpdateItemResponse) response).consumedCapacity();
          consumedValue = getConsumedCapacity(consumedCapacity);
        } else {
          consumedValue = 0;
          name = null;
        }
        if (name != null) {
          String metricName = "DynamoDb.ConsumedCapacity";
          String key = String.format("%s/%s", metricName, name);
          measure(key, metricName, tags, (int) consumedValue);
        }
        if (count > -1) {
          String metricName = "DynamoDb.RecordCount";
          String key = String.format("%s/%s", metricName, name);
          measure(key, metricName, tags, (int) count);
        }
        return response;
      }
    };
  }
}
