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

package com.dremio.iceberg.backend.dynamodb;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import java.net.URI;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class LocalDynamoDB implements AutoCloseable {
  private DynamoDBProxyServer server = null;

  public void start() throws Exception {
    final String[] localArgs = {"-inMemory"};
    server = ServerRunner.createServerFromCommandLineArgs(localArgs);
    server.start();
  }

  public DynamoDbClient client() {
    return DynamoDbClient.builder()
      .endpointOverride(URI.create("http://localhost:8000"))
      .region(Region.US_WEST_2)
      .build();
  }

  @Override
  public void close() throws Exception {
    if (server != null) {
      server.stop();
    }
  }
}
