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

import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.projectnessie.client.NessieClient

/**
  * Builder created via [[NessieDsl.nessie]] for Nessie-Gatling-Actions.
  *
  * Do not create an instance of this builder yourself.
  *
  * @param tag                tag as shown in Gatling for the action being built
  * @param nessieExec         the action to be executed, takes the NessieClient and Gatling Session
  * @param ignoreExceptions   whether exceptions are ignored
  * @param dontLogResponse    whether responses are not logged against Gatling and don't appear in the output
  */
case class NessieActionBuilder(
    tag: String,
    nessieExec: Option[(NessieClient, Session) => Session] = None,
    ignoreExceptions: Boolean = false,
    dontLogResponse: Boolean = false,
    exceptionHandler: (Exception, NessieClient, Session) => Session =
      (_, _, session) => session
) extends ActionBuilder
    with NameGen {

  /** Exceptions thrown from the [[nessieExec]] will not be propagated, but an exception handler
    * should be added via [[onException()]]. */
  def ignoreException(): NessieActionBuilder =
    NessieActionBuilder(
      tag,
      nessieExec,
      ignoreExceptions = true,
      dontLogResponse,
      exceptionHandler
    )

  /** Adds an exception handler to deal with exceptions ignored via [[ignoreException()]]. */
  def onException(
      handler: (Exception, NessieClient, Session) => Session
  ): NessieActionBuilder =
    NessieActionBuilder(
      tag,
      nessieExec,
      ignoreExceptions,
      dontLogResponse,
      handler
    )

  /** Do not push the measurement for this action to Gatling nor to Prometheus. */
  def dontLog(): NessieActionBuilder =
    NessieActionBuilder(
      tag,
      nessieExec,
      ignoreExceptions,
      dontLogResponse = true,
      exceptionHandler
    )

  /** Execute code with the current [[NessieClient]] and Gatling [[Session]]. */
  def execute(
      nessieExec: (NessieClient, Session) => Session
  ): NessieActionBuilder =
    NessieActionBuilder(
      tag,
      Some(nessieExec),
      ignoreExceptions,
      dontLogResponse,
      exceptionHandler
    )

  /** Execute code with the current [[NessieClient]], this is a convenience implementation
    * if you do not need the Gatling session in the action code. */
  def execute(nessieExec: NessieClient => Unit): NessieActionBuilder =
    execute((client, session) => {
      nessieExec.apply(client)
      session
    })

  /** Build the [[NessieAction]]. */
  def build(ctx: ScenarioContext, next: Action): Action = {
    val nessieComponents = ctx.protocolComponentsRegistry.components(
      NessieProtocol.NessieProtocolKey
    )
    val exec = nessieExec.get
    NessieAction(
      genName(s"Nessie-$tag"),
      next,
      nessieComponents,
      exec,
      ignoreExceptions,
      dontLogResponse,
      exceptionHandler
    )
  }
}

/** Use the [[NessieActionBuilder]] to create an instance of this class! */
private case class NessieAction(
    name: String,
    next: Action,
    nessieComponents: NessieComponents,
    nessieExec: (NessieClient, Session) => Session,
    ignoreExceptions: Boolean,
    dontLogResponse: Boolean,
    exceptionHandler: (Exception, NessieClient, Session) => Session
) extends ExitableAction {
  override def clock: Clock = nessieComponents.coreComponents.clock

  override def statsEngine: StatsEngine =
    nessieComponents.coreComponents.statsEngine

  override protected def execute(session: Session): Unit = {

    val start = clock.nowMillis
    try {
      val sess = nessieExec(nessieComponents.nessieProtocol.client, session)
      val end = clock.nowMillis

      if (!dontLogResponse) {
        // Tell Gatling...
        statsEngine.logResponse(
          sess.scenario,
          List.empty,
          name,
          start,
          end,
          OK,
          None,
          None
        )
      }

      next ! sess.markAsSucceeded
    } catch {
      case e: Exception =>
        val end = clock.nowMillis

        // Measure in Prometheus
        if (!dontLogResponse) {
          // Tell Gatling...
          statsEngine.logResponse(
            session.scenario,
            List.empty,
            name,
            start,
            end,
            if (ignoreExceptions) OK else KO,
            None,
            Some(e.toString)
          )
        }

        if (ignoreExceptions) {
          next ! exceptionHandler(
            e,
            nessieComponents.nessieProtocol.client,
            session
          ).markAsSucceeded
        } else {
          next ! exceptionHandler(
            e,
            nessieComponents.nessieProtocol.client,
            session
          ).markAsFailed
        }
    }
  }
}
