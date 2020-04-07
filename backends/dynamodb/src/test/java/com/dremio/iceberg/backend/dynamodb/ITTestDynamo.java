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

import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.ImmutableTable;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.VersionedWrapper;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/**
 * Dynamo db serializer test.
 */
@SuppressWarnings("AbbreviationAsWordInName")
public class ITTestDynamo {

  private static final LocalDynamoDB SERVER = new LocalDynamoDB();
  private Backend backend;

  @BeforeAll
  public static void start() throws Exception {
    SERVER.start();
    DynamoDbEnhancedClient ec = DynamoDbEnhancedClient.builder()
                                                      .dynamoDbClient(SERVER.client())
                                                      .build();
    BeanTableSchema<com.dremio.iceberg.backend.dynamodb.model.Table> schema = TableSchema.fromBean(
        com.dremio.iceberg.backend.dynamodb.model.Table.class);
    DynamoDbTable<com.dremio.iceberg.backend.dynamodb.model.Table> table = ec.table(
        "IcebergAlleyTables",
        schema);
    table.createTable();
  }

  @AfterAll
  public static void stop() throws Exception {
    SERVER.close();
  }

  @BeforeEach
  public void client() {
    backend = new DynamoDbBackend(SERVER.client());
  }

  @AfterEach
  public void close() {
    ((DynamoDbBackend) backend).close();
    backend = null;
  }

  @Test
  public void testCrud() {
    Table table = ImmutableTable.builder()
                                .tableName("test")
                                .baseLocation("test1")
                                .id("1")
                                .namespace(null)
                                .metadataLocation("x")
                                .build();
    backend.tableBackend().create("", new VersionedWrapper<>(table));

    Assertions.assertEquals(1, backend.tableBackend().getAll(null, false).size());

    VersionedWrapper<Table> versionedTable = backend.tableBackend().get("1");
    table = versionedTable.getObj();
    Assertions.assertEquals("test", table.getTableName());
    Assertions.assertEquals("test1", table.getBaseLocation());

    table = ImmutableTable.builder().from(table).metadataLocation("foo").build();
    backend.tableBackend().update("", versionedTable.update(table));

    versionedTable = backend.tableBackend().get("1");
    table = versionedTable.getObj();
    Assertions.assertEquals("test", table.getTableName());
    Assertions.assertEquals("test1", table.getBaseLocation());
    Assertions.assertEquals("foo", table.getMetadataLocation());

    List<VersionedWrapper<Table>> tables = backend.tableBackend()
                                                  .getAll("test", null, false);
    Assertions.assertEquals(1, tables.size());

    backend.tableBackend().remove("1");

    Assertions.assertNull(backend.tableBackend().get("1"));
    Assertions.assertTrue(backend.tableBackend().getAll(null, false).isEmpty());

  }

  @Test
  public void testOptimisticLocking() {
    Table table = ImmutableTable.builder()
                                .tableName("test")
                                .baseLocation("test1")
                                .id("1")
                                .namespace(null)
                                .metadataLocation("xxx")
                                .build();
    backend.tableBackend().create("", new VersionedWrapper<>(table));

    VersionedWrapper<Table> versionedTable1 = backend.tableBackend().get("1");
    VersionedWrapper<Table> versionedTable2 = backend.tableBackend().get("1");

    Table table1 = ImmutableTable.builder().from(versionedTable1.getObj()).sourceId("xyz").build();
    backend.tableBackend().update("", versionedTable1.update(table1));

    Table table2 = ImmutableTable.builder()
                                 .from(versionedTable2.getObj())
                                 .metadataLocation("foobar")
                                 .build();
    try {
      backend.tableBackend().update("", versionedTable2.update(table2));
      Assertions.fail();
    } catch (Throwable t) {
      Assertions.assertTrue(t instanceof ConditionalCheckFailedException);
    }
    versionedTable2 = backend.tableBackend().get("1");
    table2 = ImmutableTable.builder()
                           .from(versionedTable2.getObj())
                           .metadataLocation("foobar")
                           .build();
    backend.tableBackend().update("", versionedTable2.update(table2));

    VersionedWrapper<Table> versionedTable = backend.tableBackend().get("1");
    table = versionedTable.getObj();
    Assertions.assertEquals("test", table.getTableName());
    Assertions.assertEquals("test1", table.getBaseLocation());
    Assertions.assertEquals("foobar", table.getMetadataLocation());
    Assertions.assertEquals("xyz", table.getSourceId());

    backend.tableBackend().remove("1");
    Assertions.assertNull(backend.tableBackend().get("1"));
  }
}
