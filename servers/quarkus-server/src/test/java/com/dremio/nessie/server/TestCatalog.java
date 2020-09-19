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

package com.dremio.nessie.server;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@QuarkusTest
public class TestCatalog extends BaseTestIceberg {

  private static final String BRANCH = "test-catalog-branch";

  public TestCatalog() {
    super(BRANCH);
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void test() {
    createTable(TableIdentifier.of("foo", "bar"));
    List<TableIdentifier> tables = catalog.listTables(Namespace.of("foo"));
    Assertions.assertEquals(1, tables.size());
    Assertions.assertEquals("bar", tables.get(0).name());
    Assertions.assertEquals("foo", tables.get(0).namespace().toString());
    catalog.renameTable(TableIdentifier.of("foo", "bar"), TableIdentifier.of("foo", "baz"));
    tables = catalog.listTables(null);
    Assertions.assertEquals(1, tables.size());
    Assertions.assertEquals("baz", tables.get(0).name());
    Assertions.assertEquals("foo", tables.get(0).namespace().toString());
    catalog.dropTable(TableIdentifier.of("foo", "baz"));
    tables = catalog.listTables(Namespace.empty());
    Assertions.assertTrue(tables.isEmpty());
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    client.getTreeApi().deleteReference(client.getTreeApi().getReferenceByName(BRANCH));
    catalog.close();
    client.close();
    catalog = null;
    client = null;
  }

  private void createTable(TableIdentifier tableIdentifier) {
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get()))
                                         .fields());
    catalog.createTable(tableIdentifier, schema).location();
  }

}
