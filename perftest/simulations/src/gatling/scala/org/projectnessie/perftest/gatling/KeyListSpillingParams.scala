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

/** Options for testing effects of large live keylists on new commits
 *
 * Each put has a key which is distinct compared to all others that appear in
 * any given execution of the simulation.
 *
 * @param baseCommitCount the number of times to commit to a shared base branch, which is deleted if
 *                        necessary and (re)created once before beginning Gatling measurement
 * @param putsPerBaseCommit the number of put operations to include in each commit to the shared base branch
 * @param testCommitCount the number of times to commit to a short-lived test branch during Gatling measurement before
 *                        creating a new short-lived test branch; all short-lived test branches start from the shared
 *                        base branch
 * @param putsPerTestCommit the number of put operations to include in each commit to a short-lived test branch
 * @param targetDurationSeconds the wall-clock time, in seconds, that should elapse while Gatling performs operations
 *                              on short-lived test branches before terminating the simulation
 *
 * @see [[KeyListSpillingSimulation]]
  */
case class KeyListSpillingParams(
                                  baseCommitCount: Int,
                                  putsPerBaseCommit: Int,
                                  testCommitCount: Int,
                                  putsPerTestCommit: Int,
                                  targetDurationSeconds: Int
                                ) {

  private val prefix = "keylist_spilling_"

  def asPrintableString(): String = {
    s"""
    |   base-commit-count:   $baseCommitCount
    |   puts-per-base-commit:   $putsPerBaseCommit
    |   test-commit-count:   $testCommitCount
    |   puts-per-test-commit    $putsPerTestCommit
    |   target-duration-seconds:       $targetDurationSeconds
    |""".stripMargin
  }

  def getBaseBranchName(): String = {
    prefix + "base"
  }

  def makeTableName(session: Session): String = {
    prefix + "table"
  }

  def getTestBranchName(session: Session): String = {
    prefix + session.userId
  }
}

object KeyListSpillingParams {
  def fromSystemProperties(): KeyListSpillingParams = {
    val baseCommitCount: Int = Integer.getInteger("sim.baseCommitCount", 98).toInt
    val putsPerBaseCommit: Int = Integer.getInteger("sim.putsPerBaseCommit", 10).toInt
    val testCommitCount: Int = Integer.getInteger("sim.testCommitCount", 40).toInt
    val putsPerTestCommit: Int = Integer.getInteger("sim.putsPerTestCommit", 1).toInt
    val targetDurationSeconds: Int =
      Integer.getInteger("sim.duration.seconds", 60).toInt
    KeyListSpillingParams(
      baseCommitCount,
      putsPerBaseCommit,
      testCommitCount,
      putsPerTestCommit,
      targetDurationSeconds
    )
  }
}
