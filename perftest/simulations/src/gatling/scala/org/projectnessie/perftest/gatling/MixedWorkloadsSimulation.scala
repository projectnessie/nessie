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
import io.gatling.core.structure.{
  ChainBuilder,
  PopulationBuilder,
  ScenarioBuilder
}
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}
import org.projectnessie.client.api.CommitMultipleOperationsBuilder
import org.projectnessie.error.NessieConflictException
import org.projectnessie.model.CommitMeta.fromMessage
import org.projectnessie.model.{Branch, ContentKey, IcebergTable, Operation}
import org.projectnessie.perftest.gatling.Predef.nessie
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}
import scala.jdk.CollectionConverters.SeqHasAsJava

/** Gatling simulation with three parallel scenarios.
  *
  * Preparation: populate the target branch with tables
  *
  *   1. writers - updates tables - each iteration: get-HEAD, get-content,
  *      commit, for each table
  *   1. readers - only read from tables - each iteration: get-content, for each
  *      table
  *   1. ui users - list & read tables - each iteration: get-HEAD, get-keys,
  *      get-content
  *
  * It has a bunch of configurables, see [[MixedWorkloadsParams]]
  */
class MixedWorkloadsSimulation extends Simulation {

  private val simulationParams: MixedWorkloadsParams =
    MixedWorkloadsParams.fromSystemProperties()
  private val branchTables: mutable.Map[(String, Int), Lock] =
    mutable.HashMap[(String, Int), Lock]()

  private def prepareScenario(): ScenarioBuilder = {
    scenario("Initialize")
      .exec(session => {
        for (b <- 0 until simulationParams.branches.numBranches) {
          for (t <- 0 until simulationParams.tables.activeTables) {
            val branch = simulationParams.branches.branchOf(b)
            branchTables
              .put((branch, t), new ReentrantReadWriteLock().writeLock())
          }
        }
        session
          .set("namespace", simulationParams.tables.namespace)
          .set("branchesCreated", 0)
          .set("tablesCreated", 0)
      })
      .exitHereIfFailed
      .exec(prepareInitialReference)
      .exitHereIfFailed
      .exec(
        doWhile(
          session =>
            session("tablesCreated").as[Int] < simulationParams.tables.totalTables,
          "tableNum"
        ) {
          exec(prepareTables)
        }
      )
      .exitHereIfFailed
      .exec(
        doWhile(
          session =>
            session("branchesCreated").as[Int] < simulationParams.branches.numBranches,
          "branchNum"
        ) {
          exec(prepareAdditionalReference)
        }
      )
      .exitHereIfFailed
  }

  private def prepareTables: NessieActionBuilder = {
    nessie(s"prepare - Create tables ...")
      .execute { (client, session) =>
        val tablesCreated: Int =
          session("tablesCreated").asOption[Int].getOrElse(0)
        val tables: Int = simulationParams.tablesPerCommit
        val head = session("branch").as[Branch]
        val batchNum = session("tableNum").as[Int]

        val commit: CommitMultipleOperationsBuilder = client
          .commitMultipleOperations()
          .branch(head)
          .commitMeta(
            fromMessage(
              s"Create table batch $batchNum for tables $tablesCreated .. ${tablesCreated + tables - 1}"
            )
          )
        for (t <- tablesCreated until tablesCreated + tables) {
          commit.operation(
            Operation.Put.of(
              simulationParams.tables.contentKey(t, session),
              IcebergTable.of("meta-0", 1, 2, 3, 4)
            )
          )
        }
        val updatedHead = commit.commit()

        session
          .set("tablesCreated", tablesCreated + tables)
          .set("branch", updatedHead)
      }
  }

  private def prepareInitialReference: NessieActionBuilder =
    nessie(s"prepare - Create initial branch")
      .execute { (client, session) =>
        // create the branch (errors will be ignored)
        val branch = client
          .createReference()
          .reference(Branch.of(simulationParams.branches.branchOf(0), null))
          .create()
        System.err.println(s"Created initial $branch")
        session.set("branchesCreated", 1).set("branch", branch)
      }

  private def prepareAdditionalReference: NessieActionBuilder =
    nessie(s"prepare - Create additional branch")
      .execute { (client, session) =>
        val branchesCreated: Int =
          session("branchesCreated").asOption[Int].getOrElse(0)

        val initialBranch: Branch = session("branch").as[Branch]

        // create the branch (errors will be ignored)
        val branch = client.createReference
          .sourceRefName(initialBranch.getName)
          .reference(
            Branch.of(
              simulationParams.branches.branchOf(branchesCreated),
              initialBranch.getHash
            )
          )
          .create()
        System.err.println(s"Created branch $branch from $initialBranch")

        session
          .set("branchesCreated", branchesCreated + 1)
      }

  private def writersScenario(): ScenarioBuilder = {
    scenario("writers")
      .exec(session => session.set("namespace", simulationParams.tables.namespace))
      .exec(
        nessie("writers - precheck")
          .execute { (client, session) =>
            client.getDefaultBranch
            session
          }
          .dontLog()
      )
      .exitHereIfFailed
      .exec(
        forever("iteration") {
          pace(simulationParams.writers.rateToDuration())
            .exitBlockOnFail(
              exec(session =>
                session.set("branch", simulationParams.branches.randomBranch)
              )
                .exec(performUpdate("writers"))
            )
        }
      )
  }

  private def readersScenario(): ScenarioBuilder = {
    val chain = exec(
      nessie(s"readers - Read ${simulationParams.readNumTables} table(s)").execute {
        (client, session) =>
          val tables = mutable.HashSet[ContentKey]()
          while (tables.size < simulationParams.readNumTables) {
            val tableId: Int = simulationParams.tables.randomActiveTable()
            tables.add(simulationParams.tables.contentKey(tableId, session))
          }
          client.getContent
            .refName(simulationParams.branches.randomBranch)
            .keys(tables.toSeq.asJava)
            .get()
          session
      }
    )

    scenario("readers")
      .exec(session => session.set("namespace", simulationParams.tables.namespace))
      .exec(
        nessie("readers - precheck")
          .execute { (client, session) =>
            client.getDefaultBranch
            session
          }
          .dontLog()
      )
      .exitHereIfFailed
      .exec(
        forever("iteration") {
          pace(simulationParams.readers.rateToDuration()).exitBlockOnFail(chain)
        }
      )
  }

  private def uiUsersScenario(): ScenarioBuilder = {
    val chain =
      exec(session => session.set("branch", simulationParams.branches.randomBranch))
        .exec(
          nessie("ui-users - Get all entries").execute { (client, session) =>
            val branch: String = session("branch").as[String]
            // Consume all entries
            client.getEntries.refName(branch).stream().forEach(_ => {})
            session
          }
        )
        .exec(performUpdate("ui-users"))

    scenario("ui-users")
      .exec(session => session.set("namespace", simulationParams.tables.namespace))
      .exec(
        nessie("ui-users - precheck")
          .execute { (client, session) =>
            client.getDefaultBranch
            session
          }
          .dontLog()
      )
      .exitHereIfFailed
      .exec(
        forever("iteration") {
          pace(simulationParams.uiUsers.rateToDuration()).exitBlockOnFail(chain)
        }
      )
  }

  private def borrowTable(
      branch: String,
      session: Session
  ): (ContentKey, Option[Lock]) = {
    var tableLock: Option[Lock] = None
    var tableId: Int = 0
    if (!simulationParams.allowConflicts) {
      while (tableLock.isEmpty) {
        tableId = simulationParams.tables.randomActiveTable()
        val lock: Lock = branchTables((branch, tableId))
        if (lock.tryLock()) {
          tableLock = Some(lock)
        }
      }
    } else {
      tableId = simulationParams.tables.randomActiveTable()
    }
    (simulationParams.tables.contentKey(tableId, session), tableLock)
  }

  private def performUpdate(parent: String): ChainBuilder = {
    exec(session => session.set("updated", false))
      .doWhile(s => !s("updated").as[Boolean], "retries") {
        exec(session => {
          val branch: String = session("branch").as[String]
          val tableAndLock: (ContentKey, Option[Lock]) =
            borrowTable(branch, session)
          session.set("table", tableAndLock)
        })
          .exec(
            nessie(s"$parent - Update table / get content").execute {
              (client, session) =>
                val branch: String = session("branch").as[String]
                val tableAndLock: (ContentKey, Option[Lock]) =
                  session("table").as[(ContentKey, Option[Lock])]
                val content = client.getContent
                  .refName(branch)
                  .getSingle(tableAndLock._1)

                session
                  .set("content", content.getContent)
                  .set("ref", content.getEffectiveReference)
            }
          )
          .exec(
            nessie(s"$parent - Update table / commit").execute {
              (client, session) =>
                val tableAndLock: (ContentKey, Option[Lock]) =
                  session("table").as[(ContentKey, Option[Lock])]
                val ref: Branch = session("ref").as[Branch]

                // Consume all entries
                val currentTable = session("content").as[IcebergTable]
                val updatedTable = IcebergTable
                  .builder()
                  .from(currentTable)
                  .snapshotId(
                    ThreadLocalRandom.current().nextLong(1, Long.MaxValue)
                  )
                  .build()

                try {
                  client
                    .commitMultipleOperations()
                    .branch(ref)
                    .commitMeta(fromMessage(s"Update table ${tableAndLock._1}"))
                    .operation(
                      Operation.Put
                        .of(tableAndLock._1, updatedTable)
                    )
                    .commit()
                  session.set("updated", true)
                } catch {
                  case _: NessieConflictException => session
                } finally {
                  tableAndLock._2.foreach(l => l.unlock())
                }
            }
          )
      }
  }

  /** Sets up the simulation. Implemented as a function to respect the optional
    * maximum-duration.
    */
  private def doSetUp(): SetUp = {
    val nessieProtocol: NessieProtocol = nessie().clientFromSystemProperties()

    System.err.println(
      s"Setting up ${simulationParams.tables}, ${simulationParams.branches}"
    )
    val prepare: PopulationBuilder =
      prepareScenario().inject(atOnceUsers(1))

    val popBuilders: ListBuffer[PopulationBuilder] = ListBuffer()
    if (simulationParams.writers.users > 0) {
      System.err.println(s"Simulating writers: ${simulationParams.writers}")
      popBuilders.addOne(
        writersScenario().inject(atOnceUsers(simulationParams.writers.users))
      )
    }
    if (simulationParams.readers.users > 0) {
      System.err.println(s"Simulating readers: ${simulationParams.readers}")
      popBuilders.addOne(
        readersScenario().inject(atOnceUsers(simulationParams.readers.users))
      )
    }
    if (simulationParams.uiUsers.users > 0) {
      System.err.println(s"Simulating ui-users: ${simulationParams.uiUsers}")
      popBuilders.addOne(
        uiUsersScenario().inject(atOnceUsers(simulationParams.uiUsers.users))
      )
    }

    System.err.println(s"Will run for ${simulationParams.duration}")
    setUp(prepare.andThen(popBuilders))
      .maxDuration(FiniteDuration(simulationParams.duration.toNanos, NANOSECONDS))
      .protocols(nessieProtocol)
  }

  // This is where everything starts, doSetUp() returns the `SetUp` ...
  doSetUp()
}
