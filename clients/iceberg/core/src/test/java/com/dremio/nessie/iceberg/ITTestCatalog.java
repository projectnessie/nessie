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
package com.dremio.nessie.iceberg;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ITTestCatalog extends BaseTestIceberg {

  private static final String BRANCH = "test-catalog-branch";

  public ITTestCatalog() {
    super(BRANCH);
  }

  @Test
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
}
