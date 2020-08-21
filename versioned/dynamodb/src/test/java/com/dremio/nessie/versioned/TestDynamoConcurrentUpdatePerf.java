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
package com.dremio.nessie.versioned;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class TestDynamoConcurrentUpdatePerf {

  private static final String TABLE = "jacques-perf";
  private static final int COUNT = 150;
  private static final int UPDATES = 30;
  private static final AttributeValue KEY = AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[10])).build();

  private final AtomicLong failures = new AtomicLong(0);

  @Test
  public void test1() throws InterruptedException, ExecutionException {
    DynamoDbAsyncClient client = DynamoDbAsyncClient.create();
    Map<String,AttributeValue> itemValues = new HashMap<String,AttributeValue>();

    // Add all content to the table
    itemValues.put("id", KEY);
    List<Runnable> threads = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(COUNT);
    for(int i =0; i < COUNT; i++ ) {
      itemValues.put("f" + Integer.toString(i), AttributeValue.builder().n("0").build());
      threads.add(new Worker(client, latch, 0, i));
    }
    PutItemRequest put = PutItemRequest.builder().tableName(TABLE).item(itemValues).build();


    client.putItem(put).get();

    ExecutorService service = Executors.newCachedThreadPool();
    Stopwatch sw = Stopwatch.createStarted();
    threads.forEach(t -> service.submit(t));

    latch.await();
    System.out.println(String.format("Total Completed %d updates, %d failed. Took %dms.", UPDATES*COUNT, failures.get(), sw.elapsed(TimeUnit.MILLISECONDS)));
  }

  private class Worker implements Runnable {
    private final DynamoDbAsyncClient client;
    private final CountDownLatch latch;
    private int currentValue;
    private final int attribute;
    private int failCount;

    public Worker(DynamoDbAsyncClient client, CountDownLatch latch, int currentValue, int attribute) {
      super();
      this.client = client;
      this.latch = latch;
      this.currentValue = currentValue;
      this.attribute = attribute;
    }

    @Override
    public void run() {
      Thread.currentThread().setName("worker " + attribute);
      Stopwatch sw = Stopwatch.createStarted();
      for(int i =0; i < UPDATES; i++) {
        try {
          UpdateItemRequest update = UpdateItemRequest.builder()
              .key(ImmutableMap.<String, AttributeValue>of("id", KEY))
              .tableName(TABLE)
              .conditionExpression(String.format("f%d = :current", attribute))
              .updateExpression(String.format("SET f%d = :new", attribute))
              .expressionAttributeValues(ImmutableMap.<String, AttributeValue>builder()
                  .put(":new", AttributeValue.builder().n(Integer.toString(currentValue + 1)).build())
                  .put(":current", AttributeValue.builder().n(Integer.toString(currentValue)).build())
                  .build())
              //.attributeUpdates(ImmutableMap.<String, AttributeValueUpdate>of(Integer.toString(attribute), AttributeValueUpdate.builder().action(AttributeAction.PUT).value(AttributeValue.builder().n(Integer.toString(currentValue + 1)).build()).build()))
              .build();
          client.updateItem(update).get();
          currentValue++;
        } catch(Exception ex) {
          ex.printStackTrace();
          failCount++;
          failures.incrementAndGet();
        }
      }
      System.out.println(String.format("Completed %d updates, %d failed. Took %dms.", UPDATES, failCount, sw.elapsed(TimeUnit.MILLISECONDS)));
      latch.countDown();
    }


  }
}
