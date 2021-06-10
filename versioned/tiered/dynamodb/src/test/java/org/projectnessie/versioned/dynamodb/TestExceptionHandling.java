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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BackendLimitExceededException;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.store.StoreOperationException;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException;

public class TestExceptionHandling {
  @Test
  void testRequestLimitExceededException() {
    testException(
        "testRequestLimitExceededException",
        RequestLimitExceededException.builder().message("msg").build(),
        BackendLimitExceededException.class,
        "Dynamo request-limit exceeded during testRequestLimitExceededException.");
  }

  @Test
  void testLimitExceededException() {
    testException(
        "testLimitExceededException",
        LimitExceededException.builder().message("msg").build(),
        BackendLimitExceededException.class,
        "Dynamo limit exceeded during testLimitExceededException.");
  }

  @Test
  void testProvisionedThroughputExceededException() {
    testException(
        "testProvisionedThroughputExceededException",
        ProvisionedThroughputExceededException.builder().message("msg").build(),
        BackendLimitExceededException.class,
        "Dynamo provisioned throughput exceeded during testProvisionedThroughputExceededException.");
  }

  @Test
  void testStoreException() {
    testException(
        "testStoreException",
        new StoreOperationException("msg"),
        StoreOperationException.class,
        "msg");
    testException(
        "testStoreException",
        new ConditionFailedException("msg"),
        ConditionFailedException.class,
        "msg");
    testException(
        "testStoreException", new NotFoundException("msg"), NotFoundException.class, "msg");
    testException(
        "testStoreException",
        new BackendLimitExceededException("msg"),
        BackendLimitExceededException.class,
        "msg");
    testException("testStoreException", new StoreException("msg"), StoreException.class, "msg");
  }

  @Test
  void testRuntimeException() {
    testException(
        "testRuntimeException", new RuntimeException("foo bar"), RuntimeException.class, "foo bar");
  }

  private void testException(
      String operation,
      RuntimeException e,
      Class<?> expectedExceptionClass,
      String expectedMessage) {
    RuntimeException result = DynamoStore.unhandledException(operation, e);
    Assertions.assertSame(expectedExceptionClass, result.getClass());
    Assertions.assertEquals(expectedMessage, result.getMessage());
  }
}
