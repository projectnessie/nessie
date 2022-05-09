/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.base;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;

public abstract class AbstractRestGCTest extends AbstractRestGC {

  static final String CID_ONE = "cid_1";
  static final String CID_TWO = "cid_2";
  static final String CID_THREE = "cid_3";

  static final String TABLE_ONE = "table_1";
  static final String TABLE_TWO = "table_2";
  static final String TABLE_THREE = "table_3";
  static final String TABLE_TWO_RENAMED = "table_2_renamed";

  static final String METADATA_ZERO = "file0";
  static final String METADATA_ONE = "file1";
  static final String METADATA_TWO = "file2";
  static final String METADATA_THREE = "file3";
  static final String METADATA_FOUR = "file4";
  static final String METADATA_FIVE = "file5";

  @Test
  public void testSingleRefMultiTable() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t1        |            TABLE_ONE : 42 (expired)
    //         t2        |            TABLE_TWO : 42 (expired)
    //         t3        |            TABLE_TWO : 43
    //         t4        |            TABLE_ONE : 43
    //         t5        | ------- cut off time ----------------|
    //         t6        |            TABLE_TWO : 44
    //         t7        |            DROP TABLE_TWO

    String prefix = "singleRefMultiTable";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_ONE on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);
    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // both commits are expected to be expired as it is before the cutoff time and not the head
    // commits after cutoff time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 2, expectedResult);
    // one commit for TABLE_TWO on branch1, before cutoff time but as head commit for TABLE_TWO.
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);
    // one commit for TABLE_ONE on branch1, before cutoff time but as head commit for TABLE_ONE
    commitSingleOp(
        prefix, branch1, table1.hash, 43, CID_ONE, TABLE_ONE, METADATA_TWO, table1.content, null);

    final Instant cutoffTime = Instant.now();

    // one commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            44,
            CID_TWO,
            TABLE_TWO,
            METADATA_THREE,
            table2.content,
            null);

    // drop table TABLE_TWO.
    // this should not affect as it is done after the cutoff timestamp
    dropTableCommit(prefix, branch1, table2.hash, TABLE_TWO);

    // test GC with commit protection. No commits should be expired.
    performGc(prefix, cutoffTime, null, Collections.emptyList(), false, null);

    // test GC without commit protection.
    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefRenameTable() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 -----------|
    //         t0        |            create branch
    //         t1        |            TABLE_TWO : 42 (expired)
    //         t2        |            TABLE_TWO : 43
    //         t3        | ------- cut off time ---------|
    //         t4        |            TABLE_TWO : 44
    //         t5        |            RENAME TABLE_TWO to TABLE_TWO_RENAMED
    String prefix = "singleRefRenameTable";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // expected to be expired as it is before the cutoff time and not the head commit after cutoff
    // time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 1, expectedResult);

    // commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);

    final Instant cutoffTime = Instant.now();

    // commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            44,
            CID_TWO,
            TABLE_TWO,
            METADATA_THREE,
            table2.content,
            null);

    // Rename table TABLE_TWO to "table_2_renamed" on branch1.
    // Note that passing "beforeRename" argument.
    // So, internally commitSingleOp will do put + delete operation.
    commitSingleOp(
        prefix,
        branch1,
        table2.hash,
        44,
        CID_TWO,
        TABLE_TWO_RENAMED,
        METADATA_THREE,
        table2.content,
        TABLE_TWO);

    // rename table should not expire the live commits after cutoff timestamp.
    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefRenameTableBeforeCutoff() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 -------------------------------|
    //         t0        |            create branch
    //         t1        |            TABLE_TWO : 42 (expired)
    //         t2        |            TABLE_TWO : 43
    //         t3        |            RENAME TABLE_TWO to TABLE_TWO_RENAMED
    //         t4        | ------- cut off time -----------------------------|
    //         t5        |            TABLE_TWO_RENAMED : 44

    String prefix = "singleRefRenameTableBeforeCutoff";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // expected to be expired as it is before the cutoff time and not the head commit after cutoff
    // time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 1, expectedResult);

    // commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);

    // Rename table TABLE_TWO to "table_2_renamed" on branch1.
    // Note that passing "beforeRename" argument.
    // So, internally commitSingleOp will do put + delete operation.
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO_RENAMED,
            METADATA_TWO,
            table2.content,
            TABLE_TWO);

    final Instant cutoffTime = Instant.now();

    // commit for TABLE_TWO_RENAMED on branch1
    commitSingleOp(
        prefix,
        branch1,
        table2.hash,
        44,
        CID_TWO,
        TABLE_TWO_RENAMED,
        METADATA_THREE,
        table2.content,
        null);

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefDropTable() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t1        |            TABLE_ONE : 42
    //         t2        |            TABLE_TWO : 42 (expired)
    //         t3        |            TABLE_TWO : 43 (expired)
    //         t4        |            DROP TABLE_TWO
    //         t5        | ------- cut off time ----------------|
    //         t6        |            TABLE_ONE : 43
    String prefix = "singleRefDropTable";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_ONE on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);
    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // one commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);
    // both commits on table2 are expected to be expired due to drop table before cutoff time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 2, expectedResult);

    // drop table TABLE_TWO.
    dropTableCommit(prefix, branch1, table2.hash, TABLE_TWO);

    final Instant cutoffTime = Instant.now();

    // one commit for TABLE_ONE on branch1
    commitSingleOp(
        prefix, branch1, table1.hash, 43, CID_ONE, TABLE_ONE, METADATA_TWO, table1.content, null);

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefDropTableSingleTable() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t2        |            TABLE_TWO : 42 (expired)
    //         t3        |            TABLE_TWO : 43 (expired)
    //         t4        |            DROP TABLE_TWO
    //         t5        | ------- cut off time ----------------|
    String prefix = "singleRefDropTableSingleTable";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);
    // both commits on table2 are expected to be expired due to drop table before cutoff time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 2, expectedResult);

    // drop table TABLE_TWO.
    dropTableCommit(prefix, branch1, table2.hash, TABLE_TWO);

    final Instant cutoffTime = Instant.now();

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testInvalidSnapshotFiltering() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t2        |            TABLE_TWO : -1
    //         t3        |            TABLE_TWO : 42
    //         t5        | ------- cut off time ----------------|
    String prefix = "singleRefDropTableSingleTable";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), -1, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            42,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);

    final Instant cutoffTime = Instant.now();
    // expect nothing to be expired as -1 is considered as invalid snapshot for expiry.
    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefDropRefBeforeCutoff() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t1        |            TABLE_ONE : 42 (expired)
    //         t2        |            TABLE_TWO : 42 (expired)
    //         t3        |            TABLE_TWO : 43 (expired)
    //         t4        |            delete branch
    //         t5        | ------- cut off time ----------------|
    String prefix = "singleRefDropRefBeforeCutoff";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_ONE on branch1
    commitSingleOp(
        prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);
    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // one commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);
    // all 3 commits on branch1 are expected to be expired due to drop reference before cutoff time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 3, expectedResult);

    // delete branch before cutoff time
    deleteBranch(branch1.getName(), table2.hash);

    final Instant cutoffTime = Instant.now();

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefDropRefAfterCutoff() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t1        |            TABLE_ONE : 42
    //         t2        |            TABLE_TWO : 42 (expired)
    //         t3        |            TABLE_TWO : 43
    //         t4        | ------- cut off time ----------------|
    //         t5        |            delete branch

    String prefix = "singleRefDropRefAfterCutoff";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_ONE on branch1
    commitSingleOp(
        prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);
    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // after cutoff time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 1, expectedResult);
    // one commit for TABLE_TWO on branch1
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);

    final Instant cutoffTime = Instant.now();

    // delete branch before cutoff time
    deleteBranch(branch1.getName(), table2.hash);

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testSingleRefDeadRefCutoff() throws BaseNessieClientServerException {
    // ------  Time ---- | ---------- branch1 ------------------|
    //         t0        |            create branch
    //         t1        |            TABLE_ONE : 42
    //         t4        | ------- default cut off time --------|
    //         t2        |            TABLE_TWO : 42 (expired)
    //         t3        |            TABLE_TWO : 43 (expired)
    //         t3        |            TABLE_TWO : 44
    //         t4        | ------- dead ref cut off time -------|
    //         t5        |            delete branch

    String prefix = "singleRefDeadRefCutoff";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix);
    // one commit for TABLE_ONE on branch1
    commitSingleOp(
        prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);

    final Instant cutoffTime = Instant.now();

    // two commits for TABLE_TWO on branch1
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);
    // last two commits are expected to be expired as it is not the commit head
    // and commit is before dead ref cutoff time.
    fillExpectedContents(Branch.of(branch1.getName(), table2.hash), 2, expectedResult);

    table2 =
        commitSingleOp(
            prefix,
            branch1,
            table2.hash,
            44,
            CID_TWO,
            TABLE_TWO,
            METADATA_THREE,
            table2.content,
            null);

    final Instant deadRefCutoffTime = Instant.now();

    // delete branch before cutoff time
    deleteBranch(branch1.getName(), table2.hash);

    performGc(prefix, cutoffTime, null, expectedResult, true, deadRefCutoffTime);
  }

  @Test
  public void testMultiRefSharedTable() throws BaseNessieClientServerException {
    // ------  Time ---- | --- branch1 -----| ---- branch2 -----| --- branch3 -----|
    //         t0        | create branch    |                   |                  |
    //         t1        | TABLE_ONE : 41   | {TABLE_ONE : 41}  | {TABLE_ONE : 41} | --(expired)
    //         t2        | TABLE_ONE : 42   | {TABLE_ONE : 42}  | {TABLE_ONE : 42} |
    //         t3        |                  | create branch     |                  |
    //         t4        |                  |                   | create branch    |
    //         t5        |                  |  TABLE_ONE : 43   |                  |
    //         t6        |-- cut off time --|-- cut off time -- |-- cut off time --|
    //         t7        |  TABLE_ONE : 44  |                   |                  |
    //         t8        |                  |  TABLE_ONE : 45   |                  |
    //         t9        |                  |                   | TABLE_ONE : 46   |
    //         t10       |                  |                   | delete branch    |
    //         t11       |  DROP TABLE_ONE  |                   |                  |

    String prefix = "multiRefSharedTable";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix + "_1");
    // commit for TABLE_ONE on branch1
    CommitOutput b1table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 41, CID_ONE, TABLE_ONE, METADATA_ZERO, null, null);
    // expired as it is before cutoff time and not the commit head
    fillExpectedContents(Branch.of(branch1.getName(), b1table1.hash), 1, expectedResult);
    String firstCommitHash = b1table1.hash;
    b1table1 =
        commitSingleOp(
            prefix,
            branch1,
            b1table1.hash,
            42,
            CID_ONE,
            TABLE_ONE,
            METADATA_ONE,
            b1table1.content,
            null);
    Branch branch2 = createBranch(prefix + "_2", Branch.of(branch1.getName(), b1table1.hash));
    // expired as it is before cutoff time and not the commit head
    fillExpectedContents(Branch.of(branch2.getName(), firstCommitHash), 1, expectedResult);
    Branch branch3 = createBranch(prefix + "_3", Branch.of(branch1.getName(), b1table1.hash));
    // expired as it is before cutoff time and not the commit head
    fillExpectedContents(Branch.of(branch3.getName(), firstCommitHash), 1, expectedResult);
    // commit for TABLE_ONE on branch2
    CommitOutput b2table1 =
        commitSingleOp(
            prefix,
            branch2,
            branch2.getHash(),
            43,
            CID_ONE,
            TABLE_ONE,
            METADATA_TWO,
            b1table1.content,
            null);

    final Instant cutoffTime = Instant.now();

    // commit for TABLE_ONE on branch1
    b1table1 =
        commitSingleOp(
            prefix,
            branch1,
            b1table1.hash,
            44,
            CID_ONE,
            TABLE_ONE,
            METADATA_THREE,
            b2table1.content,
            null);

    // commit for TABLE_ONE on branch2
    b2table1 =
        commitSingleOp(
            prefix,
            branch2,
            b2table1.hash,
            45,
            CID_ONE,
            TABLE_ONE,
            METADATA_FOUR,
            b1table1.content,
            null);

    // commit for TABLE_ONE on branch3
    CommitOutput b3table1 =
        commitSingleOp(
            prefix,
            branch3,
            branch3.getHash(),
            46,
            CID_ONE,
            TABLE_ONE,
            METADATA_FIVE,
            b2table1.content,
            null);
    // delete branch3 should not affect as it is performed after cutoff timestamp.
    deleteBranch(branch3.getName(), b3table1.hash);

    // drop table TABLE_ONE on branch1 should not affect as it is performed after cutoff timestamp.
    dropTableCommit(prefix, branch1, b1table1.hash, TABLE_ONE);

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testMultiRefMultipleSharedTables() throws BaseNessieClientServerException {
    // ------  Time ---- | --- branch1 -----| ---- branch2 -----| --- branch3 ------------- |
    //         t0        | create branch    |                   |                           |
    //         t1        | TABLE_ONE : 42   | {TABLE_ONE : 42}  | {TABLE_ONE : 42}          |
    //         t2        |                  |  create branch    |                           |
    //         t3        | TABLE_TWO : 42   |                   | {TABLE_TWO : 42}          |
    //         t4        |                  |                   | create branch             |
    //         t5        |                  |                   | TABLE_THREE : 42 (expired)|
    //         t6        |                  |  TABLE_ONE : 43   |                           |
    //         t7        | DROP TABLE_ONE   |                   |                           |
    //         t8        |                  |                   | DROP TABLE_TWO            |
    //         t9        |                  |                   | DROP TABLE_THREE          |
    //         t10       |-- cut off time --|-- cut off time -- |-- cut off time -- --------|
    //         t11       |  TABLE_TWO : 44  |                   |                           |
    //         t12       |                  |                   | TABLE_ONE : 44            |

    String prefix = "multiRefMultipleSharedTables";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix + "_1");

    // commit on branch1
    CommitOutput b1table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);

    Branch branch2 = createBranch(prefix + "_2", Branch.of(branch1.getName(), b1table1.hash));

    // commit on branch1
    CommitOutput b1table2 =
        commitSingleOp(
            prefix, branch1, b1table1.hash, 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);

    Branch branch3 = createBranch(prefix + "_3", Branch.of(branch1.getName(), b1table2.hash));

    // commit on branch3
    CommitOutput b3table3 =
        commitSingleOp(
            prefix,
            branch3,
            branch3.getHash(),
            42,
            CID_THREE,
            TABLE_THREE,
            METADATA_ONE,
            null,
            null);
    // expect to be expired as this table is dropped before cutoff time.
    fillExpectedContents(Branch.of(branch3.getName(), b3table3.hash), 1, expectedResult);

    // commit on branch2
    CommitOutput b2table1 =
        commitSingleOp(
            prefix,
            branch2,
            branch2.getHash(),
            43,
            CID_ONE,
            TABLE_ONE,
            METADATA_TWO,
            b1table1.content,
            null);

    CommitOutput b1 = dropTableCommit(prefix, branch1, b1table2.hash, TABLE_ONE);
    CommitOutput b3 = dropTableCommit(prefix, branch3, b3table3.hash, TABLE_TWO);
    b3 = dropTableCommit(prefix, branch3, b3.hash, TABLE_THREE);

    final Instant cutoffTime = Instant.now();

    // commit for TABLE_TWO on branch1
    commitSingleOp(
        prefix, branch1, b1.hash, 44, CID_TWO, TABLE_TWO, METADATA_THREE, b1table2.content, null);

    // commit for TABLE_ONE on branch3
    commitSingleOp(
        prefix, branch3, b3.hash, 44, CID_ONE, TABLE_ONE, METADATA_FIVE, b2table1.content, null);

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }

  @Test
  public void testMultiRefCutoffTimeStampPerRef() throws BaseNessieClientServerException {
    // ------  Time ---- | --- branch1 -------------| ---- branch2 -----------   |
    //         t0        | create branch            |                            |
    //         t1        |                          | create branch              |
    //         t2        | TABLE_ONE : 42           |                            |
    //         t3        |                          |  TABLE_TWO : 42 (expired)  |
    //         t4        |-- cut off time --------  |                            |
    //         t5        |  TABLE_ONE : 44          |                            |
    //         t6        |  TABLE_ONE : 45          |                            |
    //         t7        |                          |  TABLE_TWO : 44            |
    //         t8        |                          |-- cut off time ----------- |
    //         t9        |                          | TABLE_TWO : 45             |
    String prefix = "multiRefCutoffTimeStampPerRef";
    List<Row> expectedResult = new ArrayList<>();

    Branch branch1 = createBranch(prefix + "_1");
    Branch branch2 = createBranch(prefix + "_2");
    // commit for TABLE_ONE on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);

    // commit for TABLE_TWO on branch2
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch2, branch2.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // expect this commit to be expired as it is before cutoff time and is not commit head.
    fillExpectedContents(Branch.of(branch2.getName(), table2.hash), 1, expectedResult);

    Instant defaultCutoffTime = Instant.now();

    // commits for TABLE_ONE
    table1 =
        commitSingleOp(
            prefix,
            branch1,
            table1.hash,
            44,
            CID_ONE,
            TABLE_ONE,
            METADATA_THREE,
            table1.content,
            null);
    commitSingleOp(
        prefix, branch1, table1.hash, 45, CID_ONE, TABLE_ONE, METADATA_FOUR, table1.content, null);

    // commits for TABLE_TWO
    table2 =
        commitSingleOp(
            prefix,
            branch2,
            table2.hash,
            44,
            CID_TWO,
            TABLE_TWO,
            METADATA_THREE,
            table2.content,
            null);

    Instant branch2CutoffTime = Instant.now();
    Map<String, Instant> perRefCutoffTime = new HashMap<>();
    perRefCutoffTime.put(branch2.getName(), branch2CutoffTime);

    // commits for TABLE_TWO
    commitSingleOp(
        prefix, branch2, table2.hash, 45, CID_TWO, TABLE_TWO, METADATA_FOUR, table2.content, null);

    performGc(prefix, defaultCutoffTime, perRefCutoffTime, expectedResult, true, null);
  }

  @Test
  public void testMultiRefAssignAndDropRef() throws BaseNessieClientServerException {
    // ------  Time ---- | --- branch1 -------------| ---- branch2 -----------   |
    //         t0        | create branch            |                            |
    //         t1        |                          |  create branch             |
    //         t2        | TABLE_ONE : 42 (expired) |                            |
    //         t3        |                          |  TABLE_TWO : 42 (expired)  |
    //         t4        | TABLE_ONE : 43           |                            |
    //         t5        |                          |  TABLE_TWO : 43            |
    //         t6        |-- cut off time --------  |-- cut off time --------    |
    //         t7        |  TABLE_ONE : 44          |                            |
    //         t8        |                          |  TABLE_TWO : 44            |
    //         t9        |                          | assign main to this branch |
    //         t10       |  delete branch           |                            |

    String prefix = "multiRefAssignAndDropRef";
    List<Row> expectedResult = new ArrayList<>();

    // 'before' is same as main branch
    Branch before = createBranch(prefix + "_0");
    Branch branch1 = createBranch(prefix + "_1");
    Branch branch2 = createBranch(prefix + "_2");
    // commit for TABLE_ONE on branch1
    CommitOutput table1 =
        commitSingleOp(
            prefix, branch1, branch1.getHash(), 42, CID_ONE, TABLE_ONE, METADATA_ONE, null, null);
    // expect this commit to be expired as it is before cutoff time and not the head commit
    fillExpectedContents(Branch.of(branch1.getName(), table1.hash), 1, expectedResult);
    // commit for TABLE_TWO on branch2
    CommitOutput table2 =
        commitSingleOp(
            prefix, branch2, branch2.getHash(), 42, CID_TWO, TABLE_TWO, METADATA_ONE, null, null);
    // expect this commit to be expired as it is before cutoff time and not the head commit
    fillExpectedContents(Branch.of(branch2.getName(), table2.hash), 1, expectedResult);

    table1 =
        commitSingleOp(
            prefix,
            branch1,
            table1.hash,
            43,
            CID_ONE,
            TABLE_ONE,
            METADATA_TWO,
            table1.content,
            null);
    table2 =
        commitSingleOp(
            prefix,
            branch2,
            table2.hash,
            43,
            CID_TWO,
            TABLE_TWO,
            METADATA_TWO,
            table2.content,
            null);

    final Instant cutoffTime = Instant.now();

    // commits for TABLE_ONE on branch 1
    table1 =
        commitSingleOp(
            prefix,
            branch1,
            table1.hash,
            44,
            CID_ONE,
            TABLE_ONE,
            METADATA_THREE,
            table1.content,
            null);

    // commits for TABLE_TWO on branch2
    table2 =
        commitSingleOp(
            prefix,
            branch2,
            table2.hash,
            44,
            CID_TWO,
            TABLE_TWO,
            METADATA_THREE,
            table2.content,
            null);

    // assign main to branch2 should not affect as it is performed after cutoff.
    getApi()
        .assignBranch()
        .branch(Branch.of(branch2.getName(), table2.hash))
        .assignTo(before)
        .assign();

    // drop ref branch1 should not affect as it is performed after cutoff timestamp.
    deleteBranch(branch1.getName(), table1.hash);

    performGc(prefix, cutoffTime, null, expectedResult, true, null);
  }
}
