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

import io.gatling.core.session.Session
import java.lang.Integer.getInteger
import java.lang.System.getProperty
import java.util.concurrent.ThreadLocalRandom
import org.projectnessie.model.{ContentKey, Namespace}
import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, NANOSECONDS}

case class SessionsAndRate(
    users: Int,
    opRate: Double
) {
  def rateToDuration(): FiniteDuration = {
    val oneHour = FiniteDuration(1, HOURS)
    val nanosPerIteration = oneHour.toNanos / (opRate * oneHour.toSeconds)
    FiniteDuration(nanosPerIteration.toLong, NANOSECONDS)
  }

  override def toString: String = s"$users users at rate $opRate"
}

object SessionsAndRate {
  def fromSystemProperties(
      prefix: String,
      usersDefault: Int = 1,
      opRateDefault: Double = 10
  ): SessionsAndRate = {
    val users: Int = getInteger(s"$prefix.users", usersDefault)
    val rate: Double = getProperty(s"$prefix.rate", s"$opRateDefault").toDouble
    SessionsAndRate(users, rate)
  }
}

case class MixedWorkloadsTables(
    namespaceStr: String,
    totalTables: Int,
    activeTables: Int,
    suffix: String = ""
) {
  def contentKey(t: Int, session: Session): ContentKey = {
    val namespace = session("namespace").as[Namespace]
    ContentKey.of(namespace, s"table-${t}$suffix")
  }

  def randomActiveTable(): Int = {
    ThreadLocalRandom.current().nextInt(activeTables)
  }

  def namespace: Namespace = Namespace.fromPathString(namespaceStr)

  override def toString: String =
    s"$totalTables total tables, $activeTables active, in namespace '$namespaceStr', table name suffix '$suffix'"
}

object MixedWorkloadsTables {
  def fromSystemProperties(): MixedWorkloadsTables = {
    val namespace: String = getProperty("sim.namespace", "")
    val totalTables: Int = getInteger("sim.totalTables", 10)
    val activeTables: Int = getInteger("sim.activeTables", 1)

    MixedWorkloadsTables(namespace, totalTables, activeTables)
  }
}

case class MixedWorkloadsBranches(
    branch: String,
    numBranches: Int = 1
) {
  def branchOf(num: Int): String = {
    if (num == 0) branch else s"$branch-$num"
  }

  def randomBranch: String = {
    val num =
      if (numBranches == 1) 0
      else ThreadLocalRandom.current().nextInt(numBranches)
    branchOf(num)
  }

  override def toString: String =
    s"branch prefix '$branch', $numBranches branches"
}

object MixedWorkloadsBranches {

  def fromSystemProperties(): MixedWorkloadsBranches = {
    val defaultPre = s"mixed-${System.currentTimeMillis()}"
    val branch: String = getProperty("sim.branch", defaultPre)
    val numBranches: Int = getInteger("sim.numBranches", 1)

    MixedWorkloadsBranches(branch, numBranches)
  }
}

case class MixedWorkloadsParams(
    tablePrefix: String,
    branches: MixedWorkloadsBranches,
    duration: Duration,
    tables: MixedWorkloadsTables,
    tablesPerCommit: Int,
    readNumTables: Int,
    allowConflicts: Boolean,
    writers: SessionsAndRate,
    readers: SessionsAndRate,
    uiUsers: SessionsAndRate
)

object MixedWorkloadsParams {
  def fromSystemProperties(): MixedWorkloadsParams = {

    val tablePrefix: String = getProperty("sim.tablePrefix", "table")
    val allowConflicts: Boolean =
      getProperty("sim.allowConflicts", "true").toBoolean
    val duration: Duration = Duration.create(getProperty("sim.duration", "20s"))

    val writers: SessionsAndRate =
      SessionsAndRate.fromSystemProperties("sim.writers")
    val readers: SessionsAndRate =
      SessionsAndRate.fromSystemProperties("sim.readers")
    val uiUsers: SessionsAndRate =
      SessionsAndRate.fromSystemProperties("sim.uiUsers")

    val tablesPerCommit: Int = getInteger("sim.tablesPerCommit", 20)
    val readNumTables: Int = getInteger("sim.readNumTables", 1)

    val tables = MixedWorkloadsTables.fromSystemProperties()
    val branches = MixedWorkloadsBranches.fromSystemProperties()

    MixedWorkloadsParams(
      tablePrefix,
      branches,
      duration,
      tables,
      tablesPerCommit,
      readNumTables,
      allowConflicts,
      writers,
      readers,
      uiUsers
    )
  }
}
