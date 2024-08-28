/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.dynamodb2;

import static java.util.Collections.singletonMap;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.BATCH_WRITE_MAX_REQUESTS;
import static org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Constants.KEY_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

final class BatchWrite implements AutoCloseable {
  private final DynamoDB2Backend backend;
  private final String tableName;
  private final List<WriteRequest> requestItems = new ArrayList<>();

  BatchWrite(DynamoDB2Backend backend, String tableName) {
    this.backend = backend;
    this.tableName = tableName;
  }

  private void addRequest(WriteRequest.Builder request) {
    requestItems.add(request.build());
    if (requestItems.size() == BATCH_WRITE_MAX_REQUESTS) {
      flush();
    }
  }

  void addDelete(AttributeValue key) {
    addRequest(WriteRequest.builder().deleteRequest(b -> b.key(singletonMap(KEY_NAME, key))));
  }

  public void addPut(Map<String, AttributeValue> item) {
    addRequest(WriteRequest.builder().putRequest(b -> b.item(item)));
  }

  // close() is actually a flush, implementing AutoCloseable for easier use of BatchDelete using
  // try-with-resources.
  @Override
  public void close() {
    if (!requestItems.isEmpty()) {
      flush();
    }
  }

  private void flush() {
    backend.client().batchWriteItem(b -> b.requestItems(singletonMap(tableName, requestItems)));
    requestItems.clear();
  }
}
