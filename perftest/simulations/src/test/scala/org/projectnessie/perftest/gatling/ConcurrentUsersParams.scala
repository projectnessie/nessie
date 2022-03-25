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

/** Parameters for simulating user-used scenarios where users arrive at certain
  * rates.
  *
  * The simulation runs in three phases:
  *   1. The warm-up phase where only a small number of users is inject.
  *   1. The ramm-up phase where the number of users injected scales up to the
  *      "plateau" level.
  *   1. The plateau phase where the target number of concurrent users is inject
  *      for some period of time.
  * @param usersPerSecond
  *   user arrival rate during the "plateau" phase. System property
  *   `sim.plateau.rate`.
  * @param warmUpUsersPerSecond
  *   user arrival rate during the "warm-up" phase. System property
  *   `sim.warmup.rate`.
  * @param plateauSeconds
  *   "plateau" phase duration in seconds. System property
  *   `sim.plateau.seconds`.
  * @param warmUpSeconds
  *   "warm-up" phase duration in seconds. System property `sim.warmup.seconds`.
  * @param rampUpSeconds
  *   "ramp-up" phase duration in seconds. System property `sim.rampup.seconds`.
  */
case class ConcurrentUsersParams(
    warmUpSeconds: Int,
    rampUpSeconds: Int,
    plateauSeconds: Int,
    warmUpUsersPerSecond: Double,
    usersPerSecond: Double
) {

  def asPrintableString(): String = {
    s"""
       |   warm-up user per sec:  $warmUpUsersPerSecond
       |   warm-up seconds:       $warmUpSeconds
       |   ramp-up seconds:       $rampUpSeconds
       |   plateau seconds:       $plateauSeconds
       |   plateau users per sec: $usersPerSecond
       |""".stripMargin
  }
}

object ConcurrentUsersParams {
  def fromSystemProperties(): ConcurrentUsersParams = {
    ConcurrentUsersParams(
      Integer.getInteger("sim.warmup.seconds", 1).toInt,
      Integer.getInteger("sim.rampup.seconds", 1).toInt,
      Integer.getInteger("sim.plateau.seconds", 1).toInt,
      Integer.getInteger("sim.warmup.rate", 1).toDouble,
      Integer.getInteger("sim.plateau.rate", 1).toDouble
    )
  }
}
