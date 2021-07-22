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
import org.projectnessie.client.NessieClient
import org.projectnessie.model._
import org.projectnessie.perftest.gatling.Predef.nessie

import scala.concurrent.duration.{FiniteDuration, HOURS, NANOSECONDS}

/** Gatling simulation to perform commits against Nessie.
  * Has a bunch of configurables, see the `val`s defined at the top of this class.
  */
class CreateManyBranchesSimulation extends Simulation {

  val params: CreateManyBranchesParams =
    CreateManyBranchesParams.fromSystemProperties()

  /**
    * Get the [[Branch]] object, create the branch in Nessie if needed.
    */
  private def getReference: ChainBuilder = {
    // If we don't have a reference for the branch yet, then try to create the branch and try to fetch the reference
    exec(
      nessie(s"Get main branch")
        .execute { (client, session) =>
          // create the branch (errors will be ignored)
          val defaultBranch = client.getTreeApi.getDefaultBranch
          session.set("defaultBranch", defaultBranch)
        }
        // don't measure/log this action
        .dontLog()
    )
  }

  /** Creates a Nessie branch. This is the action that we want to measure. */
  private def createBranch: ChainBuilder = {
    val chain = exec(
      nessie("Create branch")
        .execute { (client, session) =>
          val defaultBranch: Branch = session("defaultBranch").as[Branch]
          val branchNum: Int = session("branchNum").asOption[Int].get
          val branchName = s"branch-${session.userId}-$branchNum"
          client.getTreeApi.createReference(
            defaultBranch.getName,
            Branch.of(branchName, defaultBranch.getHash)
          )
          session
        }
    )

    if (params.opRate > 0) {
      // "pace" the commits, if commit-rate is configured
      val oneHour = FiniteDuration(1, HOURS)
      val nanosPerIteration = oneHour.toNanos / (params.opRate * oneHour.toSeconds)
      pace(FiniteDuration(nanosPerIteration.toLong, NANOSECONDS))
        .exitBlockOnFail(chain)
    } else {
      // if no commit-rate is configured, run "as fast as possible"
      chain
    }
  }

  private def buildScenario(): ScenarioBuilder = {
    val scn = scenario("Commit-To-Branch")
      .exec(getReference)

    // Process configured number of commits
    scn.repeat(params.numberOfBranches, "branchNum") {
      createBranch
    }
  }

  /** Sets up the simulation. Implemented as a function to respect the optional maximum-duration.
    */
  private def doSetUp(): SetUp = {
    val nessieProtocol: NessieProtocol = nessie()
      .client(
        NessieClient
          .builder()
          .withUri("http://127.0.0.1:19120/api/v1")
          .fromSystemProperties()
          .build()
      )

    System.out.println(params.asPrintableString())

    val s: SetUp = setUp(buildScenario().inject(atOnceUsers(params.numUsers)))
    s.protocols(nessieProtocol)
  }

  // This is where everything starts, doSetUp() returns the `SetUp` ...

  doSetUp()
}
