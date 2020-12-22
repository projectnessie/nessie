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

import static com.dremio.nessie.client.NessieConfigConstants.CONF_NESSIE_REF;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;

public class ITTestBranchHash extends BaseTestIceberg {

  private static final String BRANCH = "test-branch-hash";

  public ITTestBranchHash() {
    super(BRANCH);
  }

  @Test
  public void testBasicBranch() throws NessieNotFoundException, NessieConflictException {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");

    Table bar = createTable(foobar, 1); //table 1
    catalog.refresh();
    createBranch("test", catalog.getHash());

    hadoopConfig.set(CONF_NESSIE_REF, "test");

    NessieCatalog newCatalog = new NessieCatalog(hadoopConfig);
    String initialMetadataLocation = getContent(newCatalog, foobar);
    Assertions.assertEquals(initialMetadataLocation, getContent(catalog, foobar));

    bar.updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getContent(catalog, foobar), getContent(newCatalog, foobar));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getContent(newCatalog, foobar));


    String mainHash = tree.getReferenceByName(BRANCH).getHash();
    //catalog created with ref and no hash points to same catalog as above
    NessieCatalog refCatalog = new NessieCatalog(hadoopConfig, "test");
    Assertions.assertEquals(getContent(newCatalog, foobar), getContent(refCatalog, foobar));
    //catalog created with ref and hash points to 
    NessieCatalog refHashCatalog = new NessieCatalog(hadoopConfig, mainHash);
    Assertions.assertEquals(getContent(catalog, foobar), getContent(refHashCatalog, foobar));

    // asking for table@branch gives expected regardless of catalog
    Assertions.assertEquals(getContent(newCatalog, foobar), getContent(catalog, TableIdentifier.of("foo", "bar@test")));
    // asking for table@branch#hash gives expected regardless of catalog
    Assertions.assertEquals(getContent(catalog, foobar), getContent(catalog, TableIdentifier.of("foo", "bar@" + mainHash)));
  }

}
