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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.iceberg.NessieCatalog;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@QuarkusTest
class TestCatalogBranch extends BaseTestIceberg {

  public TestCatalogBranch() {
    super("main");
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testBasicBranch() throws NessieNotFoundException, NessieConflictException {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    Table bar = createTable(foobar, 1); //table 1
    createTable(foobaz, 1); //table 2
    catalog.refresh();
    createBranch("test", catalog.getHash());

    hadoopConfig.set(NessieCatalog.CONF_NESSIE_REF, "test");

    NessieCatalog newCatalog = new NessieCatalog(hadoopConfig);
    String initialMetadataLocation = getBranch(newCatalog, foobar);
    Assertions.assertEquals(initialMetadataLocation, getBranch(catalog, foobar));
    Assertions.assertEquals(getBranch(newCatalog, foobaz), getBranch(catalog, foobaz));
    bar.updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getBranch(catalog, foobar), getBranch(newCatalog, foobar));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getBranch(newCatalog, foobar));
    initialMetadataLocation = getBranch(newCatalog, foobaz);


    newCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getBranch(catalog, foobaz), getBranch(newCatalog, foobaz));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getBranch(catalog, foobaz));

    newCatalog.assignReference("main", client.getTreeApi().getReferenceByName("main").getHash(), newCatalog.getHash());
    Assertions.assertEquals(getBranch(newCatalog, foobar),
                            getBranch(catalog, foobar));
    Assertions.assertEquals(getBranch(newCatalog, foobaz),
                            getBranch(catalog, foobaz));
    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    newCatalog.refresh();
    catalog.deleteBranch("test", newCatalog.getHash());
  }

}
