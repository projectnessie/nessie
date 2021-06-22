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
package org.projectnessie.client.rest;

import javax.ws.rs.WebApplicationException;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.common.NessieVersion;

public class NessieVersionFilters {

  public static void register(HttpClient client) {
    client.register(
        (RequestContext c) -> {
          c.putHeader("Nessie-Version", NessieVersion.NESSIE_VERSION.toString());
        });
    client.register(
        (ResponseContext r) -> {
          String remoteNessieVersion = r.getHeader("Nessie-Version");
          if (remoteNessieVersion == null) {
            throw new WebApplicationException(
                "Response from Nessie-Server misses the Nessie-Version header, "
                    + "considering server as incompatible");
          }
          if (!NessieVersion.isApiCompatible(remoteNessieVersion)) {
            throw new WebApplicationException(
                String.format(
                    "Response Nessie-Server version %s is incompatible "
                        + "with local client version %s, requires server version %s",
                    remoteNessieVersion,
                    NessieVersion.NESSIE_VERSION,
                    NessieVersion.NESSIE_MIN_API_VERSION));
          }
        });
  }
}
