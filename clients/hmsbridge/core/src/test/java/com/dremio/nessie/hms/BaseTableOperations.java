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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.hamcrest.Matchers;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.klarna.hiverunner.HiveRunnerExtension;

@ExtendWith(HiveRunnerExtension.class)
public abstract class BaseTableOperations extends BaseHiveOps {

  @Test
  public void invalidTypes() {
    assertThrows("immutable=true", Exception.class, () -> shell.execute("create external table t2 (a int, b int) PARTITIONED BY (c int);"));
    assertThrows("External Tables", Exception.class, () -> shell.execute("create table t2 (a int, b int) PARTITIONED BY (c int);"));
  }

  @Test
  public void insertOnImmutable() {
    shell.execute("create external table t1 (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES (\"immutable\"=\"true\");");
    shell.execute("insert into t1 PARTITION(c)"
        + "select 1 as a,1 as b,1 as c "
        + "union all select 2,2,2 "
        + "union all select 3,3,3 "
        + "union all select 4,4,4 ");
    List<String> partitions = shell.executeQuery("show partitions t1");
    assertThat(partitions, Matchers.containsInAnyOrder(new String[] {"c=1","c=2","c=3","c=4"}));

    shell.execute("alter table t1 drop partition (c=1)");
    List<String> partitions2 = shell.executeQuery("show partitions t1");
    assertThat(partitions2, Matchers.containsInAnyOrder(new String[] {"c=2","c=3","c=4"}));

    // TODO, fix infinite loop in drop partitions. Need to expose correct transactional property for table.
    shell.execute("drop table t1");
  }

  @Test
  public void changeContext() throws NessieNotFoundException, NessieConflictException {

    String mainOriginalHash = client.getTreeApi().getDefaultBranch().getHash();

    // create a table on main and populate it with data.
    shell.execute("create external table t1 (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES (\"immutable\"=\"true\");");
    shell.execute("insert into t1 PARTITION(c) "
        + "select 1 as a,1 as b,1 as c "
        + "union all select 2,2,2 "
        + "union all select 3,3,3 "
        + "union all select 4,4,4 ");

    // create a new branch.
    shell.execute(String.format("CREATE VIEW `$nessie`.dev TBLPROPERTIES(\"ref\"=\"%s\") AS SELECT * FROM T1", mainOriginalHash));

    // change to dev context using pseudo database
    shell.execute("alter database `$nessie` set dbproperties (\"ref\"=\"dev\")");

    // create a different version of same table on dev branch
    shell.execute("create external table t1 (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES (\"immutable\"=\"true\");");

    // check that the dev version of the table is empty.
    assertEquals(0, shell.executeQuery("select * from `t1`").size());

    // check that the main table still has it's data.
    assertEquals(4, shell.executeQuery("select * from `t1@main`").size());

    // switch main context to the main branch.
    shell.execute("alter database `$nessie` set dbproperties (\"ref\"=\"main\")");

    // check that the local reference now points to the populated table.
    assertEquals(4, shell.executeQuery("select * from `t1`").size());

    // query the table from master.
    assertEquals(0, shell.executeQuery("select * from `t1@dev`").size());

    shell.execute("drop table t1");

    shell.execute("alter database `$nessie` set dbproperties (\"ref\"=\"dev\")");
    shell.execute("drop table t1");

    Object[] items = shell.executeQuery("show tables in `$nessie`").toArray();

    assertThat(Arrays.asList("main", "dev"), Matchers.containsInAnyOrder(items));
  }

  @Test
  public void tableCreateDrop() {
    shell.execute("create external table t1 (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES (\"immutable\"=\"true\");");
    List<Object[]> records = shell.executeStatement("describe t1");
    shell.execute("DROP TABLE t1");
  }

  private static <T extends Throwable> T assertThrows(String expectedMessageFragment, Class<T> clazz, Executable e) {
    T ex = Assertions.assertThrows(clazz, e);
    assertThat(ex.getMessage(), StringContains.containsString(expectedMessageFragment));
    return ex;
  }

}
