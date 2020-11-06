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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class LocalDynamoDB extends TypeBasedParameterResolver<DynamoDbClient> implements AfterAllCallback, BeforeAllCallback {

  private static final String DYNAMODB_CLIENT = "dynamodb-local-client";

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    Holder holder = getHolder(context, true);
    if (holder != null) {
      return;
    }
    getStore(context).put(DYNAMODB_CLIENT, new Holder());
  }

  private static class Holder {

    private final DynamoDBProxyServer server;
    private final DynamoDbClient client;

    public Holder() throws Exception {
      final String[] localArgs = {"-inMemory"};
      server = ServerRunner.createServerFromCommandLineArgs(localArgs);
      server.start();
      client = DynamoDbClient.builder()
          .httpClient(UrlConnectionHttpClient.create())
          .endpointOverride(URI.create("http://localhost:8000"))
          .region(Region.US_WEST_2)
          .build();
      client.listTables();
    }

    private void stop() throws Exception {
      client.close();
      server.stop();
    }

  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
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
      Holder holder = (Holder) getStore(c).get(DYNAMODB_CLIENT);
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
  public DynamoDbClient resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
    return getHolder(extensionContext, true).client;
  }

}
