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
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.Clock
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import io.micrometer.core.instrument.{MeterRegistry, Timer}
import io.opentracing.Scope
import io.opentracing.log.Fields
import io.opentracing.tag.Tags

import java.time.Duration
import scala.jdk.CollectionConverters._

/**
 * Builds a Nessie-Gatling-Action.
 * @param tag tag as shown in Gatling for the action being built
 * @param nessieExec the action to be executed, takes the NessieClient and Gatling Session
 * @param ignoreExceptions whether exceptions are ignored
 * @param dontLogResponse whether responses are not logged against Gatling and don't appear in the output
 * @param withTracing whether the action shall be traced against Jaeger
 * @param traceScopeEnricher code to add custom data to the trace-scope
 */
case class NessieActionBuilder(tag: String, nessieExec: Option[(NessieClient, Session) => Session] = None,
                               ignoreExceptions: Boolean = false, dontLogResponse: Boolean = false,
                               withTracing: Boolean = false, traceScopeEnricher: (Scope, Session) => Unit = (_, _) => {},
                               exceptionHandler: (Exception, NessieClient, Session) => Session = (_, _, session) => session
                              ) extends ActionBuilder with NameGen {
  def ignoreException(): NessieActionBuilder = NessieActionBuilder(tag, nessieExec, ignoreExceptions = true, dontLogResponse, withTracing, traceScopeEnricher, exceptionHandler)

  def onException(handler: (Exception, NessieClient, Session) => Session): NessieActionBuilder = NessieActionBuilder(tag, nessieExec, ignoreExceptions, dontLogResponse, withTracing, traceScopeEnricher, handler)

  def trace(scopeEnricher: (Scope, Session) => Unit): NessieActionBuilder = NessieActionBuilder(tag, nessieExec, ignoreExceptions, dontLogResponse, withTracing = true, scopeEnricher, exceptionHandler)

  def trace(): NessieActionBuilder = trace((_, _) => {})

  def dontLog(): NessieActionBuilder = NessieActionBuilder(tag, nessieExec, ignoreExceptions, dontLogResponse = true, withTracing, traceScopeEnricher, exceptionHandler)

  def execute(nessieExec: (NessieClient, Session) => Session): NessieActionBuilder = NessieActionBuilder(tag, Some(nessieExec), ignoreExceptions, dontLogResponse, withTracing, traceScopeEnricher, exceptionHandler)

  def execute(nessieExec: NessieClient => Unit): NessieActionBuilder = execute((client, session) => {
    nessieExec.apply(client)
    session
  })

  def build(ctx: ScenarioContext, next: Action): Action = {
    val nessieComponents = ctx.protocolComponentsRegistry.components(NessieProtocol.NessieProtocolKey)
    val exec = nessieExec.get
    NessieAction(genName(s"Nessie: $tag"), next, nessieComponents, exec, ignoreExceptions, dontLogResponse, withTracing, traceScopeEnricher, exceptionHandler)
  }
}

case class NessieAction(name: String, next: Action, nessieComponents: NessieComponents,
                        nessieExec: (NessieClient, Session) => Session, ignoreExceptions: Boolean,
                        dontLogResponse: Boolean,
                        withTracing: Boolean, traceScopeEnricher: (Scope, Session) => Unit,
                        exceptionHandler: (Exception, NessieClient, Session) => Session) extends ExitableAction {
  override def clock: Clock = nessieComponents.coreComponents.clock

  override def statsEngine: StatsEngine = nessieComponents.coreComponents.statsEngine

  private def startTrace: Option[Scope] = {
    if (withTracing && nessieComponents.tracer.isDefined) {
      Some(nessieComponents.tracer.map(tracer => tracer.buildSpan(name).startActive(true)).get)
    } else {
      None
    }
  }

  private def metric(sample: Timer.Sample, session: Session, status: String, error: Boolean, registry: MeterRegistry): Unit = {
    sample.stop(Timer.builder("nessie.benchmark.action")
      .tags(nessieComponents.nessieProtocol.prometheusPush.get.commonTags.asJava)
      .tag("action", name)
      .tag("scenario", session.scenario)
      .tag("status", status)
      .tag("error", error.toString)
      .publishPercentileHistogram()
      .distributionStatisticBufferLength(3)
      .distributionStatisticExpiry(Duration.ofMinutes(1))
      .register(registry))
  }

  override protected def execute(session: Session): Unit = {

    val scope = startTrace
    scope.foreach(s => traceScopeEnricher(s, session))

    val sample = Timer.start()
    val registry = session("prometheus.registry").as[MeterRegistry]
    val start = clock.nowMillis
    try {
      val sess = nessieExec(nessieComponents.nessieProtocol.client, session)
      val end = clock.nowMillis

      if (!dontLogResponse) {
        // Measure in Prometheus
        metric(sample, session, "OK", error = false, registry)

        // Tell Gatling...
        statsEngine.logResponse(sess.scenario, List.empty, name, start, end, OK, None, None)
      }

      // close tracing scope
      scope.foreach(s => s.close())

      next ! sess.markAsSucceeded
    } catch {
      case e: Exception =>
        val end = clock.nowMillis

        // Measure in Prometheus
        if (!dontLogResponse) {
          // Measure in Prometheus, do not add the exception message (Prometheus size restrictions)
          metric(sample, session, if (ignoreExceptions) "OK" else "Fail", error = true, registry)

          // Tell Gatling...
          statsEngine.logResponse(session.scenario, List.empty, name, start, end, if (ignoreExceptions) OK else KO, None, Some(e.toString))
        }

        // TODO propagate exception to caller as some check??

        // add exception information to trace + close tracing scope
        scope.foreach(s => {
          Tags.ERROR.set(s.span.log(Map(Fields.EVENT -> Tags.ERROR.getKey,
            Fields.ERROR_OBJECT -> e.toString).asJava), true)
          s.close()
        })

        if (ignoreExceptions) {
          next ! exceptionHandler(e, nessieComponents.nessieProtocol.client, session).markAsSucceeded
        } else {
          next ! exceptionHandler(e, nessieComponents.nessieProtocol.client, session).markAsFailed
        }
    }
  }
}
