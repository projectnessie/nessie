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

import java.net.URI;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.model.BranchControllerObject;
import com.dremio.nessie.model.BranchControllerReference;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Implements Backend for DynamoDB.
 */
public class DynamoDbBackend implements Backend {

  private final DynamoDbClient client;

  /**
   * Create a Backend for dynamodb. This uses Dynamodbs object mapper to interact w/ the database.
   */
  public DynamoDbBackend(DynamoDbClient client) {
    this.client = client;
  }

  public static DynamoDbBackend getInstance(String region) {
    return getInstance(region, null);
  }

  /**
   * Create instance of the backend.
   *
   * @param region   region to connect to (eg us-west-2)
   * @param endpoint endpoint (useful for localstack or other non AWS instances)
   */
  public static DynamoDbBackend getInstance(String region, String endpoint) {
    DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
    if (endpoint != null) {
      clientBuilder = clientBuilder.endpointOverride(URI.create(endpoint));
    }
    if (region != null) {
      clientBuilder = clientBuilder.region(Region.of(region));
    }

    DynamoDbClient client = clientBuilder.httpClient(UrlConnectionHttpClient.builder().build())
                                         .build();

    return new DynamoDbBackend(client);
  }

  public EntityBackend<BranchControllerObject> gitBackend() {
    return new GitObjectDynamoDbBackend(client);
  }

  @Override
  public EntityBackend<BranchControllerReference> gitRefBackend() {
    return new GitRefDynamoDbBackend(client);
  }

  @Override
  public void close() {

  }

  /**
   * Factory for DynamoDB Backend.
   */
  public static class BackendFactory implements Backend.Factory {

    @Override
    public Backend create(String region, String endpoint) {
      return DynamoDbBackend.getInstance(region,endpoint);
    }
  }

}
