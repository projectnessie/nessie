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
package org.projectnessie.catalog.api;

import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.catalog.api.model.AddDataFilesOutcome;
import org.projectnessie.catalog.api.model.AddDataFilesRequest;
import org.projectnessie.catalog.api.model.CatalogConfig;
import org.projectnessie.catalog.api.model.MergeOutcome;
import org.projectnessie.catalog.api.model.MergeRequest;
import org.projectnessie.catalog.api.model.PickTablesOutcome;
import org.projectnessie.catalog.api.model.PickTablesRequest;

@Consumes({MediaType.APPLICATION_JSON})
@jakarta.ws.rs.Consumes({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@jakarta.ws.rs.Produces({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
@Path("nessie-catalog/v0")
@jakarta.ws.rs.Path("nessie-catalog/v0")
@Tag(name = "Nessie Catalog v0")
public interface NessieCatalogV0Api {
  @GET
  @jakarta.ws.rs.GET
  @Path("config")
  @jakarta.ws.rs.Path("config")
  @Produces({MediaType.APPLICATION_JSON})
  @jakarta.ws.rs.Produces({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
  CatalogConfig config();

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/add-data-files/{namespace}/{table}")
  @jakarta.ws.rs.Path("/{prefix}/add-data-files/{namespace}/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  AddDataFilesOutcome addDataFiles(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @PathParam("table") @jakarta.ws.rs.PathParam("table") String table,
      AddDataFilesRequest addDataFiles)
      throws IOException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/pick-tables")
  @jakarta.ws.rs.Path("/{prefix}/pick-tables")
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  PickTablesOutcome pickTables(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      PickTablesRequest pickTables)
      throws IOException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/merge")
  @jakarta.ws.rs.Path("/{prefix}/merge")
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  MergeOutcome merge(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix, MergeRequest merge)
      throws IOException;
}
