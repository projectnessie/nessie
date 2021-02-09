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

import org.projectnessie.client.NessieClient
import org.projectnessie.model.{Branch, ContentsKey, IcebergTable}
import org.projectnessie.perftest.gatling.Predef.nessie
import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.ScenarioBuilder
import io.micrometer.core.instrument.Tag
import org.projectnessie.error.NessieConflictException

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.{FiniteDuration, HOURS, NANOSECONDS, SECONDS}

object BranchMode extends Enumeration {
  type BranchMode = Value
  val BRANCH_PER_USER_RANDOM_TABLE, BRANCH_PER_USER_SINGLE_TABLE, SINGLE_BRANCH_RANDOM_TABLE, SINGLE_BRANCH_TABLE_PER_USER, SINGLE_BRANCH_SINGLE_TABLE = Value
}

/**
 * Gatling simulation to perform commits against Nessie.
 *
 * Has a bunch of configurables, see the `val`s defined at the top of this class.
 */
class CommitToBranchSimulation extends Simulation {
  /**
   * The Nessie branch name to use, the default include the current wall-clock in ms since epoch.
   */
  val branchName: String = System.getProperty("sim.branch", s"commits-${System.currentTimeMillis()}")
  /**
   * Whether all simulated users commit to the same branch, defaults to each user committing to
   * its own branch (=false).
   */
  val branchMode: BranchMode.BranchMode = BranchMode.withName(System.getProperty("sim.branchMode", BranchMode.BRANCH_PER_USER_SINGLE_TABLE.toString))
  /**
   * The table name to use, the default include the current wall-clock in ms since epoch.
   */
  val tablePrefix: String = System.getProperty("sim.tablePrefix", s"someTable${System.currentTimeMillis()}")
  /**
   * The number of simulated tables, each commit chooses a random table.
   */
  val numTables: Int = Integer.getInteger("sim.tableCount", 250)
  /**
   * The number of users to simulate, defaults to 1.
   */
  val numUsers: Int = Integer.getInteger("sim.users", 1).toInt
  /**
   * The commit-rate in commits-per-second, defaults to 0 (= as fast as possible).
   * Implemented as using Gatling's "pace()" function, which acts like a "sleep()" after each
   * commit.
   */
  val commitRate: Double = System.getProperty("sim.rate", "0").toDouble
  /**
   * The number of commits to perform per user, defaults to 100.
   */
  val numberOfCommits: Int = Integer.getInteger("sim.commits", 100).toInt
  /**
   * The runtime duration (for commits) of the simulations in seconds, defaults to 0, which
   * means endless or the specified number of commits have been performed.
   */
  val durationSeconds: Int = Integer.getInteger("sim.duration.seconds", 0).toInt
  /**
   * The IP and port of the Prometheus Push-Gateway, defaults to 127.0.0.1:9091.
   * A "benchmark_description" tag is added to all metrics, see the note-property.
   */
  val prometheusPushURL: String = System.getProperty("sim.prometheus", "127.0.0.1:9091")
  /**
   * Arbitrary note added as a Prometheus tag, used to distinguish metrics from different runs
   * in e.g. Grafana.
   */
  val note: String = System.getProperty("sim.note", "")

  val nessieProtocol: NessieProtocol = nessie()
    .prometheusPush(PrometheusPush("commit_to_branch_simulation", prometheusPushURL,
      Seq(Tag.of("application", "Nessie-Benchmark")),
      Seq(Tag.of("benchmark_description", s"users=$numUsers commits=$numberOfCommits branchMode=$branchMode " +
        s"table=$tablePrefix branch=$branchName rate=$commitRate duration=${durationSeconds}s note=$note"))
    ))
    .client(NessieClient.builder()
    .withUri("http://127.0.0.1:19120/api/v1").fromSystemProperties()
    .withTracing(true).build())

  def branchName(session: Session): String = {
    val userId = session.userId
    branchMode match {
      case BranchMode.SINGLE_BRANCH_TABLE_PER_USER => branchName
      case BranchMode.SINGLE_BRANCH_RANDOM_TABLE => branchName
      case BranchMode.SINGLE_BRANCH_SINGLE_TABLE => branchName
      case BranchMode.BRANCH_PER_USER_RANDOM_TABLE => s"$branchName-$userId"
      case BranchMode.BRANCH_PER_USER_SINGLE_TABLE => s"$branchName-$userId"
    }
  }

  private def getReference = {
    exec(nessie(s"Create branch $branchName")
      .execute { (client, session) =>
        // create the branch (errors will be ignored)
        client.getTreeApi.createReference(Branch.of(branchName(session), null))
        session
      }
      // ignore any exception and do not log the "create-branch" operation
      .ignoreException().dontLog())
      .exec(nessie(s"Get reference $branchName").execute { (client, session) =>
        // retrieve the Nessie branch reference and store it in the Gatling session object
        val branch = client.getTreeApi.getReferenceByName(branchName(session)).asInstanceOf[Branch]
        session.set("branch", branch)
      })
  }

  private def commitToBranch = {
    // If we don't have a reference for the branch yet, then try to create the branch and try to fetch the reference
    val chain = exec(nessie("Commit").execute { (client, session) =>
      val userId = session.userId
      val commitNum = session("commitNum").asOption[Int].get
      val branch = session("branch").as[Branch]

      // Call the Nessie client operation to perform a commit
      val tableName = branchMode match {
        case BranchMode.BRANCH_PER_USER_RANDOM_TABLE => s"${tablePrefix}_${ThreadLocalRandom.current().nextInt(numTables)}"
        case BranchMode.BRANCH_PER_USER_SINGLE_TABLE => tablePrefix
        case BranchMode.SINGLE_BRANCH_RANDOM_TABLE => s"${tablePrefix}_${ThreadLocalRandom.current().nextInt(numTables)}"
        case BranchMode.SINGLE_BRANCH_TABLE_PER_USER => s"${tablePrefix}_${userId}"
        case BranchMode.SINGLE_BRANCH_SINGLE_TABLE => tablePrefix
      }

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
      // add user-id and commit-number to the tracing tags
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

  private def buildScenario(): ScenarioBuilder = {
    var scn = scenario("Commit-To-Branch")
      .exec(getReference)
    if (numberOfCommits > 0) {
      // Process configured number of commits
      scn = scn.repeat(numberOfCommits, "commitNum") {
        commitToBranch
      }
    } else {
      // otherwise run "forever" (or until "max-duration")
      scn = scn.forever("commitNum") {
        commitToBranch
      }
    }

    scn
  }

  /**
   * Sets up the simulation. Implemented as a function to respect the optional maximum-duration.
   */
  private def doSetUp(): SetUp = {
    System.out.println(
      s"""
        |Simulation parameters:
        |   branch-name:    $branchName
        |   branch-mode:    $branchMode
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
