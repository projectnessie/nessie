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
package org.projectnessie.perftest.gatling

import io.gatling.core.Predef.Session
import org.projectnessie.model.IcebergTable

import java.util.concurrent.ThreadLocalRandom

/** Parameters for the [[CommitToBranchSimulation]].
  *
  * @param branch The Nessie branch name to use, the default include the current wall-clock in ms since epoch.
  *               System property: `sim.branch`, defaults to `s"commits-${System.currentTimeMillis()}"`.
  * @param mode The simulation mode, defined in the [[BranchMode]] enum.
  *             System property: `sim.branchMode`, defaults to [[BranchMode.BRANCH_PER_USER_SINGLE_TABLE]].
  * @param tablePrefix The table name to use, the default include the current wall-clock in ms since epoch.
  *                    System property: `sim.tablePrefix`, defaults to `s"someTable${System.currentTimeMillis()}"`.
  * @param numTables The number of simulated tables, each commit chooses a random table
  *                  . System property: `sim.tables`, defaults to `250`.
  * @param numberOfCommits The number of commits to perform per user.
  *                        System property: `sim.commits`, defaults to `100`.
  * @param durationSeconds The runtime duration (for commits) of the simulations in seconds.
  *                        `0` means endless or until the specified number of commits
  *                        ([[numberOfCommits]]) have been performed.
  *                        System property: `sim.duration.seconds`, defaults to `0`.
  * @param numUsers see [[BaseParams.numUsers]]
  * @param opRate see [[BaseParams.opRate]]
  * @param note see [[BaseParams.note]]
  */
case class CommitToBranchParams(
    branch: String,
    mode: BranchMode.BranchMode,
    tablePrefix: String,
    numTables: Int,
    numberOfCommits: Int,
    durationSeconds: Int,
    override val numUsers: Int,
    override val opRate: Double,
    override val note: String
) extends BaseParams {

  override def asPrintableString(): String = {
    s"""${super.asPrintableString().trim}
    |   branch-name:    $branch
    |   branch-mode:    $mode
    |   table-prefix:   $tablePrefix
    |   num-tables:     $numTables
    |   num-commits:    $numberOfCommits
    |   duration:       $durationSeconds
    |""".stripMargin
  }

  /** Generate the table name for the given session depending on [[mode]], [[tablePrefix]] and [[numTables]]. */
  def makeTableName(session: Session): String = {
    mode match {
      case BranchMode.BRANCH_PER_USER_RANDOM_TABLE =>
        s"${tablePrefix}_${ThreadLocalRandom.current().nextInt(numTables)}"
      case BranchMode.BRANCH_PER_USER_SINGLE_TABLE => tablePrefix
      case BranchMode.SINGLE_BRANCH_RANDOM_TABLE =>
        s"${tablePrefix}_${ThreadLocalRandom.current().nextInt(numTables)}"
      case BranchMode.SINGLE_BRANCH_TABLE_PER_USER =>
        s"${tablePrefix}_${session.userId}"
      case BranchMode.SINGLE_BRANCH_SINGLE_TABLE => tablePrefix
    }
  }

  /** Compute branch name for a user depending on [[mode]] and [[branch]]. */
  def makeBranchName(session: Session): String = {
    mode match {
      case BranchMode.SINGLE_BRANCH_TABLE_PER_USER => branch
      case BranchMode.SINGLE_BRANCH_RANDOM_TABLE   => branch
      case BranchMode.SINGLE_BRANCH_SINGLE_TABLE   => branch
      case BranchMode.BRANCH_PER_USER_RANDOM_TABLE =>
        s"$branch-${session.userId}"
      case BranchMode.BRANCH_PER_USER_SINGLE_TABLE =>
        s"$branch-${session.userId}"
    }
  }
}

object CommitToBranchParams {
  def fromSystemProperties(): CommitToBranchParams = {
    val branch: String =
      System.getProperty("sim.branch", s"commits-${System.currentTimeMillis()}")
    val mode: BranchMode.BranchMode = BranchMode.withName(
      System
        .getProperty(
          "sim.mode",
          BranchMode.BRANCH_PER_USER_SINGLE_TABLE.toString
        )
    )
    val tablePrefix: String = System.getProperty(
      "sim.tablePrefix",
      s"someTable${System.currentTimeMillis()}"
    )
    val numTables: Int = Integer.getInteger("sim.tables", 250)
    val numberOfCommits: Int = Integer.getInteger("sim.commits", 100).toInt
    val durationSeconds: Int =
      Integer.getInteger("sim.duration.seconds", 0).toInt
    val base = BaseParams.fromSystemProperties()

    CommitToBranchParams(
      branch,
      mode,
      tablePrefix,
      numTables,
      numberOfCommits,
      durationSeconds,
      base.numUsers,
      base.opRate,
      base.note
    )
  }
}

object BranchMode extends Enumeration {
  type BranchMode = Value
  val

  /** Each simulated user "works" on its own branch and each Nessie-commit against a random (one
    * out of [[CommitToBranchParams.numTables]]) [[IcebergTable]] name.
    */
  BRANCH_PER_USER_RANDOM_TABLE,
      /** Each simulated user "works" on its own branch and all Nessie-commit against the same
    * [[IcebergTable]] name.
    */
  BRANCH_PER_USER_SINGLE_TABLE,
      /** All simulated user "work" on a single branch and each Nessie-commit against a random (one
    * out of [[CommitToBranchParams.numTables]]) [[IcebergTable]] name.
    */
  SINGLE_BRANCH_RANDOM_TABLE,
      /** All simulated user "work" on a single branch and each Nessie-commit against an
    * [[IcebergTable]] per user.
    */
  SINGLE_BRANCH_TABLE_PER_USER,
      /** All simulated user "work" on a single branch and all Nessie-commits update a single
    * [[IcebergTable]].
    */
  SINGLE_BRANCH_SINGLE_TABLE = Value
}
