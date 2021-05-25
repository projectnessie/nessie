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
package org.projectnessie.services.rest;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;
import org.projectnessie.common.NessieVersion;

@Provider
public class NessieVersionChecker implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) {
    String remoteNessieVersion = requestContext.getHeaders().getFirst("Nessie-Version");
    // Note: no-header means incompatible Nessie-Client
    if (remoteNessieVersion == null) {
      // TODO the status reason is always "Bad Request" via Quarkus - seems to be a
      // Quarkus/Resteasy-bug :(
      requestContext.abortWith(
          Response.status(
                  Status.BAD_REQUEST.getStatusCode(),
                  String.format(
                      "Remote Nessie-Client did not sent a version, considering incompatible "
                          + "with local server version %s, minimum required client version is %s",
                      NessieVersion.NESSIE_VERSION, NessieVersion.NESSIE_MIN_API_VERSION))
              .header("Nessie-Version", NessieVersion.NESSIE_VERSION)
              .build());
    } else if (!NessieVersion.isApiCompatible(remoteNessieVersion)) {
      // TODO the status reason is always "Bad Request" via Quarkus - seems to be a
      // Quarkus/Resteasy-bug :(
      requestContext.abortWith(
          Response.status(
                  Status.BAD_REQUEST.getStatusCode(),
                  String.format(
                      "Remote Nessie-Client version %s is incompatible "
                          + "with local server version %s, minimum required client version is %s",
                      remoteNessieVersion,
                      NessieVersion.NESSIE_VERSION,
                      NessieVersion.NESSIE_MIN_API_VERSION))
              .header("Nessie-Version", NessieVersion.NESSIE_VERSION)
              .build());
    }
  }
}
