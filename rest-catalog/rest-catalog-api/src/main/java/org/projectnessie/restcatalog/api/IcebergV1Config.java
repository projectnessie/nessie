/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.restcatalog.api;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** Iceberg REST API v1 JAX-RS interface, using request and response types from Iceberg. */
@Consumes({MediaType.APPLICATION_JSON})
@jakarta.ws.rs.Consumes({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@jakarta.ws.rs.Produces({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
@Path("iceberg/v1")
@jakarta.ws.rs.Path("iceberg/v1")
@Tag(name = "Iceberg v1")
public interface IcebergV1Config {

  @GET
  @jakarta.ws.rs.GET
  @Path("config")
  @jakarta.ws.rs.Path("config")
  @Produces({MediaType.APPLICATION_JSON})
  @jakarta.ws.rs.Produces({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
  ConfigResponse getConfig(
      @QueryParam("warehouse") @jakarta.ws.rs.QueryParam("warehouse") String warehouse);
}
