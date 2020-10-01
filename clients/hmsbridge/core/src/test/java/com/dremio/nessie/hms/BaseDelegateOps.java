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
package com.dremio.nessie.hms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.HiveDatabase;
import com.dremio.nessie.model.HiveTable;

public abstract class BaseDelegateOps extends BaseHiveOps {

  @Test
  public void crossNessieDelegateQuery() throws NessieNotFoundException {
    shell.execute("create table legacy (a int, b int)");
    shell.execute("insert into legacy "
        + "select 1 as a,1 as b "
        + "union all select 2,2 "
        + "union all select 3,3 "
        + "union all select 4,4 ");
    shell.execute("create database mytestdb");
    shell.execute("create external table mytestdb.nessie (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES (\"immutable\"=\"true\")");
    shell.execute("insert into mytestdb.nessie PARTITION(c)"
        + "select 1 as a,1 as b,1 as c "
        + "union all select 2,2,2 "
        + "union all select 3,3,3 "
        + "union all select 4,4,4 ");
    List<String> results = shell.executeQuery("select * from legacy l join mytestdb.nessie n on l.a = n.a");
    assertEquals(4, results.size());

    // make sure we created the database and single table object in the nessie db.
    Contents db = client.getContentsApi().getContents(ContentsKey.of("mytestdb"));
    assertNotNull(db);
    assertTrue(HiveDatabase.class.isAssignableFrom(db.getClass()));
    Contents tbl = client.getContentsApi().getContents(ContentsKey.of("mytestdb","nessie"));
    assertNotNull(tbl);
    assertTrue(HiveTable.class.isAssignableFrom(tbl.getClass()));

    // ensure only one table was created in Nessie.
    assertEquals(2, client.getTreeApi().getEntries("main").getEntries().size());
  }

}
