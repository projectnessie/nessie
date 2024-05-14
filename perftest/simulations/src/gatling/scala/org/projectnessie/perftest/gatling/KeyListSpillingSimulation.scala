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

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import org.projectnessie.error.NessieReferenceNotFoundException
import org.projectnessie.model.Operation.Put
import org.projectnessie.model._
import org.projectnessie.perftest.gatling.Predef.nessie
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.jdk.CollectionConverters._

/** Measure throughput of incremental put commits against an existing HEAD with
  * a preexisting live keylist.
  *
  * @see
  *   [[KeyListSpillingParams]]
  */
class KeyListSpillingSimulation extends Simulation {

  private val log = LoggerFactory.getLogger(classOf[KeyListSpillingSimulation])

  /** Fixed-size key padding (256 chars, repeating every 8 chars)
    */
  private val paddingBuilder: mutable.StringBuilder =
    new mutable.StringBuilder()
  for (_ <- 1 to 32) {
    paddingBuilder.append("abcdefg_")
  }
  val padding: String = paddingBuilder.toString()

  /** Configuration specific to this simulation and scenario.
    */
  private val params: KeyListSpillingParams =
    KeyListSpillingParams.fromSystemProperties()

  /** Session key for the zero-indexed commit number associated with the current
    * simulated Gatling user.
    *
    * Gatling initializes and increments this for us via [[repeat]].
    */
  private val commitIndexKey = "commitIndex"

  /** Commit once to a branch conveyed via the [[Session]].
    *
    * Although the returned chain fragment will commit only once, the number of
    * put operations in that commit is determined by
    * [[KeyListSpillingParams.putsPerTestCommit]].
    */
  private def commitToBranch: ChainBuilder = {
    exec(
      nessie("TestBranchCommit")
        .execute { (client, session) =>
          // This is initialized and incremented on our behalf by Gatling (see buildScenario)
          val commitIndex = session(commitIndexKey).as[Int]
          val commitNum = commitIndex + 1
          val testBranch = session("branch").as[Branch]
          val userId = session.userId
          val putOps: List[Operation] = List
            .range(0, params.putsPerTestCommit)
            .map(i => {
              val key = ContentKey.of(s"$userId $commitIndex $i" + padding)
              val table = IcebergTable
                .of(s"metadata_${userId}_${commitNum}_$i", 42, 43, 44, 45)
              Put.of(key, table)
            })

          // Call the Nessie client operation to perform a commit with one or more put ops
          val updatedBranch = client
            .commitMultipleOperations()
            .branch(testBranch)
            .commitMeta(
              CommitMeta.fromMessage(
                s"worker commit userId=$userId testCommitNum=$commitNum"
              )
            )
            .operations(putOps.asJava)
            .commit()

          session.set("branch", updatedBranch)
        }
    )
  }

  /** Delete a test [[Branch]] (if it exists), then (re)create it.
    *
    * The test branch's name ends in the Gatling userId (as conveyed through the
    * [[Session]].
    */
  private def dropAndCreateTestBranch: ChainBuilder = {
    exec(
      nessie("DeleteTestBranchIfExists")
        .execute { (client, session) =>
          val testBranchName = params.getTestBranchName(session)

          // Get the test branch (we must know its hash to delete it)
          try {
            val testBranch: Reference = client.getReference
              .refName(testBranchName)
              .get()
              .asInstanceOf[Branch]

            // Delete the test branch
            client
              .deleteReference()
              .refName(testBranchName)
              .hash(testBranch.getHash)
              .delete()
          } catch {
            case e: NessieReferenceNotFoundException =>
              log.debug(
                "Test branch did not exist during delete-if-exists operation" +
                  "(this is normal on a clean Nessie backend, or when changing some benchmark parameters on a dirty one)",
                e
              )
          }

          session
        }
    ).exec(
      nessie("CreateTestBranch")
        .execute { (client, session) =>
          // Get the base branch
          val parentBranch: Reference = client.getReference
            .refName(params.getBaseBranchName)
            .get()

          // Create a new test branch from the base branch
          val testBranch = client
            .createReference()
            .sourceRefName(params.getBaseBranchName)
            .reference(
              Branch.of(params.getTestBranchName(session), parentBranch.getHash)
            )
            .create()
            .asInstanceOf[Branch]

          session.set("branch", testBranch)
        }
    )
  }

  /** Constructs a scenario to prep and commit to a test branch.
    *
    * The test branch is deleted if it exists, then created. Commits to the new
    * branch are repeated [[KeyListSpillingParams.testCommitCount]] times.
    */
  private def buildScenario(): ScenarioBuilder = {
    scenario("KeyList-Spilling")
      .exec(dropAndCreateTestBranch)
      .repeat(params.testCommitCount, commitIndexKey) {
        commitToBranch
      }
  }

  /** Creates our simulation and invokes Gatling's
    * [[io.gatling.core.scenario.Simulation.setUp]]
    */
  private def doSetUp(): SetUp = {
    val nessieProtocol: NessieProtocol = nessie().clientFromSystemProperties()

    val branchName = params.getBaseBranchName

    // Drop base branch, if it exists
    try {
      val ref = nessieProtocol.client.getReference.refName(branchName).get()

      if (null != ref) {
        nessieProtocol.client
          .deleteReference()
          .reference(Branch.of(params.getBaseBranchName, ref.getHash))
          .delete()
      }
    } catch {
      case e: NessieReferenceNotFoundException =>
        log.info(
          s"Base branch $branchName does not exist, " +
            s"not attempting to delete (this is normal when running against a clean Nessie backend)",
          e
        )
    }

    // Create base branch
    nessieProtocol.client
      .createReference()
      .reference(Branch.of(params.getBaseBranchName, null))
      .create()

    // Load test fixture into base branch
    val branch = nessieProtocol.client.getReference
      .refName(branchName)
      .get()
      .asInstanceOf[Branch]
    loadData(nessieProtocol, branch)

    // Leave branch-and-commit cycles to Gatling via our scenario
    val s: SetUp = setUp(
      buildScenario().inject(
        constantConcurrentUsers(1).during(
          FiniteDuration(params.targetDurationSeconds, SECONDS)
        )
      )
    ).maxDuration(FiniteDuration(params.targetDurationSeconds, SECONDS))

    s.protocols(nessieProtocol)
  }

  /** Commit one or more puts of Iceberg Tables to the given branch, each
    * uniquely keyed.
    *
    * @param nessieProtocol
    *   the client attached to this protocol will be used for commits
    * @param branch
    *   the target branch upon which to commit
    */
  private def loadData(nessieProtocol: NessieProtocol, branch: Branch): Unit = {

    val commitCount = params.baseCommitCount
    val operationCount = params.putsPerBaseCommit

    log.info(
      "Loading shared fixture into branch {}: {} commits, each with {} distinctly-keyed put operations",
      branch,
      commitCount,
      operationCount
    )

    for (i <- 1 to commitCount) {

      val operations: List[Operation] = LazyList
        .from(i * operationCount)
        .take(operationCount)
        .map(commitNum => String.valueOf(commitNum) + " " + padding)
        .map(keyName =>
          Operation.Put.of(
            ContentKey.of(keyName),
            IcebergTable.of("/iceberg/table", 42, 42, 42, 42)
          )
        )
        .toList

      log.debug(
        "Preparing {} put operations in commit {}/{}",
        operations.size,
        i,
        commitCount
      )

      nessieProtocol.client
        .commitMultipleOperations()
        .operations(operations.asJava)
        .branch(branch)
        .commitMeta(
          CommitMeta.fromMessage(s"Base branch commit $i/$commitCount")
        )
        .commit();
    }
  }

  // Instantiate Simulation, Scenario, Protocol, etc. and return a Gatling SetUp
  doSetUp()
}
