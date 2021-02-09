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

/**
  *
  * @param numberOfBranches number of branches that each simulated user creates (defaults to 100)
  * @param numUsers see [[BaseParams.numUsers]]
  * @param opRate see [[BaseParams.opRate]]
  * @param note see [[BaseParams.note]]
  */
case class CreateManyBranchesParams(
    numberOfBranches: Int,
    override val numUsers: Int,
    override val opRate: Double,
    override val note: String
) extends BaseParams {

  override def asPrintableString(): String = {
    s"""${super.asPrintableString().trim}
    |   num-branches:   $numberOfBranches
    |""".stripMargin
  }
}

object CreateManyBranchesParams {
  def fromSystemProperties(): CreateManyBranchesParams = {
    val base = BaseParams.fromSystemProperties()
    val numberOfBranches: Int = Integer.getInteger("sim.branches", 100).toInt
    CreateManyBranchesParams(
      numberOfBranches,
      base.numUsers,
      base.opRate,
      base.note
    )
  }
}
