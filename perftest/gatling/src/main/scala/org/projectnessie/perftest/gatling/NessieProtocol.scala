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

import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.scenario.Simulation
import io.gatling.core.session.Session
import org.projectnessie.client.NessieClient

case class NessieProtocolBuilder() extends StrictLogging {

  /** Inject the [[NessieClient]] for the simulation and returns an instance of [[NessieProtocol]],
    * which is to be passed to [[Simulation.SetUp.protocols()]]. */
  def client(client: NessieClient): NessieProtocol =
    NessieProtocol(client)
}

case class NessieProtocol(
    client: NessieClient
) extends Protocol {
  type Components = NessieComponents
}

object NessieProtocol {
  val NessieProtocolKey: ProtocolKey[NessieProtocol, NessieComponents] =
    new ProtocolKey[NessieProtocol, NessieComponents] {

      def protocolClass: Class[Protocol] =
        classOf[NessieProtocol].asInstanceOf[Class[Protocol]]

      def defaultProtocolValue(
          configuration: GatlingConfiguration
      ): NessieProtocol =
        throw new IllegalStateException(
          "Can't provide a default value for NessieProtocol"
        )

      def newComponents(
          coreComponents: CoreComponents
      ): NessieProtocol => NessieComponents =
        nessieProtocol => NessieComponents(coreComponents, nessieProtocol)
    }
}

/** Gatling protocol components for Nessie, nothing useful for users in here. */
case class NessieComponents(
    coreComponents: CoreComponents,
    nessieProtocol: NessieProtocol
) extends ProtocolComponents {
  override def onStart: Session => Session = session => session

  override def onExit: Session => Unit = _ => {}
}
