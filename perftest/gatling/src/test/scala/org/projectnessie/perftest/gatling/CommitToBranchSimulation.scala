/**
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

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration.{FiniteDuration, HOURS, NANOSECONDS, SECONDS}

import org.projectnessie.client.NessieClient
import org.projectnessie.error.NessieConflictException
import org.projectnessie.model.{Branch, ContentsKey, IcebergTable}
import org.projectnessie.perftest.gatling.Predef.nessie

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.ScenarioBuilder
import io.micrometer.core.instrument.Tag

/** Gatling simulation to perform commits against Nessie.
 *  Has a bunch of configurables, see the `val`s defined at the top of this class.
 */
class CommitToBranchSimulation extends Simulation {
  /** The Nessie branch name to use, the default include the current wall-clock in ms since epoch.
   *  System property: `sim.branch`, defaults to `s"commits-${System.currentTimeMillis()}"`.
   */
  val branch: String = System.getProperty("sim.branch", s"commits-${System.currentTimeMillis()}")
  /** The simulation mode, defined in the [[BranchMode]] enum.
   *  System property: `sim.branchMode`, defaults to [[BranchMode.BRANCH_PER_USER_SINGLE_TABLE]].
   */
  val mode: BranchMode.BranchMode = BranchMode.withName(System.getProperty("sim.mode", BranchMode.BRANCH_PER_USER_SINGLE_TABLE.toString))
  /** The table name to use, the default include the current wall-clock in ms since epoch.
   *  System property: `sim.tablePrefix`, defaults to `s"someTable${System.currentTimeMillis()}"`.
   */
  val tablePrefix: String = System.getProperty("sim.tablePrefix", s"someTable${System.currentTimeMillis()}")
  /** The number of simulated tables, each commit chooses a random table.
   *  System property: `sim.tables`, defaults to `250`.
   */
  val numTables: Int = Integer.getInteger("sim.tables", 250)
  /** The number of users to simulate.
   *  System property: `sim.users`, defaults to `1`.
   */
  val numUsers: Int = Integer.getInteger("sim.users", 1).toInt
  /** The commit-rate in commits-per-second, `0` means "as fast as possible".
   *  Implemented with Gatling's [[io.gatling.core.structure.Pauses#pace(Duration)]] function,
   *  which acts like a "sleep()" after each commit.
   *  System property: `sim.rate`, defaults to `0`.
   */
  val commitRate: Double = System.getProperty("sim.rate", "0").toDouble
  /** The number of commits to perform per user.
   *  System property: `sim.commits`, defaults to `100`.
   */
  val numberOfCommits: Int = Integer.getInteger("sim.commits", 100).toInt
  /** The runtime duration (for commits) of the simulations in seconds.
   *  `0` means endless or until the specified number of commits ([[numberOfCommits]]) have been performed.
   *  System property: `sim.duration.seconds`, defaults to `0`.
   */
  val durationSeconds: Int = Integer.getInteger("sim.duration.seconds", 0).toInt
  /** The IP and port of the Prometheus Push-Gateway.
   *  A "benchmark_description" tag is added to all metrics, see the note-property.
   *  System property: `sim.prometheus`, defaults to `127.0.0.1:9091`.
   */
  val prometheusPushURL: String = System.getProperty("sim.prometheus", "127.0.0.1:9091")
  /** Arbitrary note added as a Prometheus tag, used to distinguish metrics from different runs
   *  in e.g. Grafana.
   *  System property: `sim.note`, defaults to `""`.
   */
  val note: String = System.getProperty("sim.note", "")

  /** The actual benchmark code to measure Nessie-commit performance in various scenarios. */
  private def commitToBranch = {
    val chain = exec(nessie("Commit").execute { (client, session) =>
      // The commit number is the loop-variable declared buildScenario()
      val commitNum = session("commitNum").asOption[Int].get
      // Current Nessie Branch object
      val branch = session("branch").as[Branch]
      // Our "user ID", an integer supplied by Gatling
      val userId = session.userId
      // Table used in the Nessie commit
      val tableName = makeTableName(session)

      // Call the Nessie client operation to perform a commit
      client.getContentsApi.setContents(ContentsKey.of("name", "space", tableName),
        branch.getName, branch.getHash,
        s"test-commit $userId $commitNum", IcebergTable.of(s"path_on_disk_${tableName}_$commitNum"))

      session
    }.onException { (e, client, session) =>
      if (e.isInstanceOf[NessieConflictException]) {
        val branch = session("branch").as[Branch]
        session.set("branch", client.getTreeApi.getReferenceByName(branch.getName).asInstanceOf[Branch])
      } else {
        session
      }
    }.trace((scope, session) => {
      // add user-id and commit-number to the Jaeger tracing tags
      val userId = session.userId
      val commitNum = session("commitNum").asOption[Int].get
      scope.span().setTag("sim.user-id", userId).setTag("sim.commit-num", commitNum)
    }))

    if (commitRate > 0) {
      // "pace" the commits, if commit-rate is configured
      val oneHour = FiniteDuration(1, HOURS)
      val nanosPerIteration = oneHour.toNanos / (commitRate * oneHour.toSeconds)
      pace(FiniteDuration(nanosPerIteration.toLong, NANOSECONDS)).exitBlockOnFail(chain)
    } else {
      // if no commit-rate is configured, run "as fast as possible"
      chain
    }
  }

  def makeTableName(session: Session): String = {
    mode match {
      case BranchMode.BRANCH_PER_USER_RANDOM_TABLE => s"${tablePrefix}_${ThreadLocalRandom.current().nextInt(numTables)}"
      case BranchMode.BRANCH_PER_USER_SINGLE_TABLE => tablePrefix
      case BranchMode.SINGLE_BRANCH_RANDOM_TABLE => s"${tablePrefix}_${ThreadLocalRandom.current().nextInt(numTables)}"
      case BranchMode.SINGLE_BRANCH_TABLE_PER_USER => s"${tablePrefix}_${session.userId}"
      case BranchMode.SINGLE_BRANCH_SINGLE_TABLE => tablePrefix
    }
  }

  /** Compute branch name for a user depending on [[mode]] and [[branch]] */
  def makeBranchName(session: Session): String = {
    mode match {
      case BranchMode.SINGLE_BRANCH_TABLE_PER_USER => branch
      case BranchMode.SINGLE_BRANCH_RANDOM_TABLE => branch
      case BranchMode.SINGLE_BRANCH_SINGLE_TABLE => branch
      case BranchMode.BRANCH_PER_USER_RANDOM_TABLE => s"$branch-${session.userId}"
      case BranchMode.BRANCH_PER_USER_SINGLE_TABLE => s"$branch-${session.userId}"
    }
  }

  private def getReference = {
    // If we don't have a reference for the branch yet, then try to create the branch and try to fetch the reference
    exec(nessie(s"Create branch $branch")
      .execute { (client, session) =>
        // create the branch (errors will be ignored)
        client.getTreeApi.createReference(Branch.of(makeBranchName(session), null))
        session
      }
      // ignore any exception and do not log the "create-branch" operation
      .ignoreException().dontLog())
      .exec(nessie(s"Get reference $branch").execute { (client, session) =>
        // retrieve the Nessie branch reference and store it in the Gatling session object
        val branch = client.getTreeApi.getReferenceByName(makeBranchName(session)).asInstanceOf[Branch]
        session.set("branch", branch)
      })
  }

  private def buildScenario(): ScenarioBuilder = {
    val scn = scenario("Commit-To-Branch")
      .exec(getReference)

    if (numberOfCommits > 0) {
      // Process configured number of commits
      scn.repeat(numberOfCommits, "commitNum") {
        commitToBranch
      }
    } else {
      // otherwise run "forever" (or until "max-duration")
      scn.forever("commitNum") {
        commitToBranch
      }
    }
  }

  /** Sets up the simulation. Implemented as a function to respect the optional maximum-duration.
   */
  private def doSetUp(): SetUp = {
    val nessieProtocol: NessieProtocol = nessie()
      .prometheusPush(PrometheusPush("commit_to_branch_simulation", prometheusPushURL,
        Seq(Tag.of("application", "Nessie-Benchmark")),
        Seq(Tag.of("benchmark_description", s"users=$numUsers commits=$numberOfCommits branchMode=$mode " +
          s"table=$tablePrefix branch=$branch rate=$commitRate duration=${durationSeconds}s note=$note"))
      ))
      .client(NessieClient.builder()
        .withUri("http://127.0.0.1:19120/api/v1").fromSystemProperties()
        .withTracing(true).build())

    System.out.println(
      s"""
        |Simulation parameters:
        |   branch-name:    $branch
        |   branch-mode:    $mode
        |   table-prefix:   $tablePrefix
        |   num-tables:     $numTables
        |   num-users:      $numUsers
        |   commit-rate:    $commitRate
        |   num-commits:    $numberOfCommits
        |   duration:       $durationSeconds
        |   note:           $note
        |""".stripMargin)

    var s = setUp(buildScenario().inject(atOnceUsers(numUsers)))
    if (durationSeconds > 0) {
      s = s.maxDuration(FiniteDuration(durationSeconds, SECONDS))
    }
    s.protocols(nessieProtocol)
  }

  // This is where everything starts...
  doSetUp()
}

object BranchMode extends Enumeration {
  type BranchMode = Value
  val
  /** Each simulated user "works" on its own branch and each Nessie-commit against a random (one
   *  out of [[CommitToBranchSimulation.numTables]]) [[IcebergTable]] name.
   */
  BRANCH_PER_USER_RANDOM_TABLE,
  /** Each simulated user "works" on its own branch and all Nessie-commit against the same
   *  [[IcebergTable]] name.
   */
  BRANCH_PER_USER_SINGLE_TABLE,
  /** All simulated user "work" on a single branch and each Nessie-commit against a random (one
   *  out of [[CommitToBranchSimulation.numTables]]) [[IcebergTable]] name.
   */
  SINGLE_BRANCH_RANDOM_TABLE,
  /** All simulated user "work" on a single branch and each Nessie-commit against an
   *  [[IcebergTable]] per user.
   */
  SINGLE_BRANCH_TABLE_PER_USER,
  /** All simulated user "work" on a single branch and all Nessie-commits update a single
   *  [[IcebergTable]].
   */
  SINGLE_BRANCH_SINGLE_TABLE = Value
}
