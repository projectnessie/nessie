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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.Table;

public class TestDynamo {
  private static final LocalDynamoDB server = new LocalDynamoDB();
  private Backend backend;

  @BeforeClass
  public static void start() throws Exception {
    server.start();
    DynamoDBMapper mapper = new DynamoDBMapper(server.client());
    CreateTableRequest req =
      mapper.generateCreateTableRequest(com.dremio.iceberg.backend.dynamodb.model.Table.class);
    req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
    server.client().createTable(req);
  }

  @AfterClass
  public static void stop() throws Exception {
    server.close();
  }

  @Before
  public void client() {
    backend = new DynamoDBBackend(server.client());
  }

  @After
  public void close() throws Exception {
    ((DynamoDBBackend) backend).close();
    backend = null;
  }

  @Test
  public void testCrud() {
    Table table = new Table("test", "test1");
    table.setUuid("1");
    backend.create("", table);

    Assert.assertEquals(1, backend.getAll(null, false).getTables().size());

    table = backend.get("1");
    Assert.assertEquals("test", table.getTableName());
    Assert.assertEquals("test1", table.getBaseLocation());

    table.setMetadataLocation("foo");
    backend.update("", table);

    table = backend.get("1");
    Assert.assertEquals("test", table.getTableName());
    Assert.assertEquals("test1", table.getBaseLocation());
    Assert.assertEquals("foo", table.getMetadataLocation());

    backend.remove("1");

    Assert.assertNull(backend.get("1"));
    Assert.assertTrue(backend.getAll(null, false).getTables().isEmpty());

  }

  @Test
  public void testOptimisticLocking() {
    Table table = new Table("test", "test1");
    table.setUuid("1");
    backend.create("", table);

    Table table1 = backend.get("1");
    Table table2 = backend.get("1");

    table1.setSourceId("xyz");
    backend.update("", table1);

    table2.setMetadataLocation("foobar");
    try {
      backend.update("", table1);
      Assert.fail();
    } catch (Throwable t) {
      System.out.println(t);
    }
    table2 = backend.get("1");
    table2.setMetadataLocation("foobar");
    backend.update("", table2);

    table = backend.get("1");
    Assert.assertEquals("test", table.getTableName());
    Assert.assertEquals("test1", table.getBaseLocation());
    Assert.assertEquals("foobar", table.getMetadataLocation());
    Assert.assertEquals("xyz", table.getSourceId());

    backend.remove("1");
    Assert.assertNull(backend.get("1"));
  }
}
