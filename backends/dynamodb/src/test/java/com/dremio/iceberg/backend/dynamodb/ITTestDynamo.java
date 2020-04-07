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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.Table;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/**
 * Dynamo db serializer test
 */
public class ITTestDynamo {
  private static final LocalDynamoDB SERVER = new LocalDynamoDB();
  private Backend backend;

  @BeforeClass
  public static void start() throws Exception {
    SERVER.start();
    DynamoDbEnhancedClient ec = DynamoDbEnhancedClient.builder()
      .dynamoDbClient(SERVER.client())
      .build();
    BeanTableSchema<com.dremio.iceberg.backend.dynamodb.model.Table> schema =
      TableSchema.fromBean(com.dremio.iceberg.backend.dynamodb.model.Table.class);
    DynamoDbTable<com.dremio.iceberg.backend.dynamodb.model.Table> table = ec.table("IcebergAlleyTables", schema);
    table.createTable();
  }

  @AfterClass
  public static void stop() throws Exception {
    SERVER.close();
  }

  @Before
  public void client() {
    backend = new DynamoDBBackend(SERVER.client());
  }

  @After
  public void close() throws Exception {
    ((DynamoDBBackend) backend).close();
    backend = null;
  }

  @Test
  public void testCrud() {
    Table table = Table.table("test", "test1");
    table = table.withId("1");
    backend.tableBackend().create("", table);

    Assert.assertEquals(1, backend.tableBackend().getAll(null, false).size());

    table = backend.tableBackend().get("1");
    Assert.assertEquals("test", table.getTableName());
    Assert.assertEquals("test1", table.getBaseLocation());

    table = table.withMetadataLocation("foo");
    backend.tableBackend().update("", table);

    table = backend.tableBackend().get("1");
    Assert.assertEquals("test", table.getTableName());
    Assert.assertEquals("test1", table.getBaseLocation());
    Assert.assertEquals("foo", table.getMetadataLocation());

    backend.tableBackend().remove("1");

    Assert.assertNull(backend.tableBackend().get("1"));
    Assert.assertTrue(backend.tableBackend().getAll(null, false).isEmpty());

  }

  @Test
  public void testOptimisticLocking() {
    Table table = Table.table("test", "test1");
    table = table.withId("1");
    backend.tableBackend().create("", table);

    Table table1 = backend.tableBackend().get("1");
    Table table2 = backend.tableBackend().get("1");

    table1 = table1.withSourceId("xyz");
    backend.tableBackend().update("", table1);

    table2 = table2.withMetadataLocation("foobar");
    try {
      backend.tableBackend().update("", table2);
      Assert.fail();
    } catch (Throwable t) {
      Assert.assertTrue(t instanceof ConditionalCheckFailedException);
    }
    table2 = backend.tableBackend().get("1");
    table2 = table2.withMetadataLocation("foobar");
    backend.tableBackend().update("", table2);

    table = backend.tableBackend().get("1");
    Assert.assertEquals("test", table.getTableName());
    Assert.assertEquals("test1", table.getBaseLocation());
    Assert.assertEquals("foobar", table.getMetadataLocation());
    Assert.assertEquals("xyz", table.getSourceId());

    backend.tableBackend().remove("1");
    Assert.assertNull(backend.tableBackend().get("1"));
  }
}
