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

import java.lang.reflect.Type;
import java.net.URI;
import java.util.Collections;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.GenericContainer;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class LocalDynamoDB implements ParameterResolver, AfterAllCallback, BeforeAllCallback {

  private static final String DYNAMODB_HOLDER = "dynamodb-local";
  public static final String[] LOCAL_DYNAMO_ARGS = {"-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-inMemory"};

  @Override
  public void beforeAll(ExtensionContext context) {
    Holder holder = getHolder(context, true);
    if (holder != null) {
      return;
    }
    getStore(context).put(DYNAMODB_HOLDER, new Holder());
  }

  private static class Holder {

    private final GenericContainer<?> localDynamoContainer;
    private final URI dynamoURI;

    private DynamoDbClient client;

    public Holder() {
      localDynamoContainer = new GenericContainer<>("amazon/dynamodb-local");
      localDynamoContainer.setCommandParts(LOCAL_DYNAMO_ARGS);
      localDynamoContainer.setExposedPorts(Collections.singletonList(8000));
      localDynamoContainer.start();

      dynamoURI = URI.create(String.format("http://localhost:%d", localDynamoContainer.getFirstMappedPort()));
    }

    private DynamoDbClient client() {
      if (client == null) {
        client = DynamoDbClient.builder()
            .httpClient(UrlConnectionHttpClient.create())
            .endpointOverride(dynamoURI)
            .region(Region.US_WEST_2)
            .build();
        client.listTables();
      }
      return client;
    }

    private void stop() {
      if (client != null) {
        client.close();
      }
      localDynamoContainer.stop();
    }

  }

  @Override
  public void afterAll(ExtensionContext context) {
    Holder h = getHolder(context, false);
    if (h != null) {
      h.stop();
    }
  }

  private Store getStore(ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context));
  }

  private Holder getHolder(ExtensionContext context, boolean recursive) {
    ExtensionContext c = context;
    do {
      Holder holder = (Holder) getStore(c).get(DYNAMODB_HOLDER);
      if (holder != null) {
        return holder;
      }

      if (!recursive) {
        break;
      }

      c = c.getParent().orElse(null);
    } while (c != null);

    return null;
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Type parameterType = parameterContext.getParameter().getParameterizedType();
    return DynamoDbClient.class.equals(parameterType)
        || DynamoStoreConfig.class.equals(parameterType);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
    Type parameterType = parameterContext.getParameter().getParameterizedType();
    Holder holder = getHolder(extensionContext, true);
    if (DynamoDbClient.class.equals(parameterType)) {
      return holder.client();
    }
    if (DynamoStoreConfig.class.equals(parameterType)) {
      return DynamoStoreConfig.builder()
          .endpoint(holder.dynamoURI)
          .region(Region.US_WEST_2)
          .build();
    }
    return null;
  }

}
