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
package org.projectnessie.perftest.gatling

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import org.projectnessie.client.api.NessieApiV1
import org.projectnessie.client.http.HttpClientBuilder
import org.projectnessie.model.Operation.Put
import org.projectnessie.model._
import org.projectnessie.perftest.gatling.Predef.nessie

/** Simulates multiple users performing commits against Nessie.
  *
  * The simulation runs multiple concurrent users each of which creates a new
  * (unique) table on the main branch.
  *
  * This simulation is not very realistic from the end user perspective because
  * using a new table UUID is each commit is not the intended way of using the
  * Nessie Catalog. It is generally expected that table UUIDs are reused and
  * each table is mentioned in more than one commit.
  *
  * The goal of this simulation is mostly to probe the robustness and
  * scalability of the handling of global data in Nessie under contention when
  * many table UUIDs are at play.
  *
  * This simulation is meant to run in two major modes:
  *   - Using a high request rate with a long ramp up time to probe for capacity
  *     limits.
  *   - Using a fixed, moderate request rate with warm up and a long "plateau"
  *     time to study response times.
  *
  * These modes are not really distinct, but blend one into the other depending
  * on actual parameter values.
  *
  * See [[ConcurrentUsersParams]] for the full set of simulation parameters.
  */
class ManyUniqueTablesSimulation extends Simulation {

  val params: ConcurrentUsersParams =
    ConcurrentUsersParams.fromSystemProperties()

  private def getBranch: ChainBuilder = {
    // If we don't have a reference for the branch yet, then try to create the branch and try to fetch the reference
    exec(
      nessie(s"Get Branch")
        .execute { (client, session) =>
          val branch = client.getDefaultBranch
          session.set("branch", branch)
        }
    )
  }

  private def createTable: ChainBuilder = {
    exec(
      nessie(s"Create Table")
        .execute { (client, session) =>
          val branch = session("branch").as[Branch]
          val key = ContentKey.of(s"test.table.user-${session.userId}")
          val table = IcebergTable.of(
            s"metadata-${System.currentTimeMillis()}",
            1,
            1,
            1,
            1
          )
          val newHead = client
            .commitMultipleOperations()
            .branch(branch)
            .operation(Put.of(key, table))
            .commitMeta(
              CommitMeta.fromMessage(s"test-commit ${session.userId}")
            )
            .commit()

          session.set("branch", newHead)
        }
    )
  }

  private def buildScenario(): ScenarioBuilder = {
    scenario("Concurrent Commits")
      .exec(getBranch)
      .exec(createTable)
  }

  private def doSetUp(): SetUp = {
    val nessieProtocol: NessieProtocol = nessie()
      .client(
        HttpClientBuilder
          .builder()
          .fromSystemProperties()
          .build(classOf[NessieApiV1])
      )

    println(params.asPrintableString())

    val scn = buildScenario()
      .inject(
        constantUsersPerSec(params.warmUpUsersPerSecond)
          .during(params.warmUpSeconds),
        rampUsersPerSec(params.warmUpUsersPerSecond)
          .to(params.usersPerSecond)
          .during(params.rampUpSeconds),
        constantUsersPerSec(params.usersPerSecond).during(params.plateauSeconds)
      )

    setUp(scn).protocols(nessieProtocol)
  }

  // This is where everything starts ...
  doSetUp()
}
