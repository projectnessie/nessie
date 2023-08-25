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
package org.projectnessie.catalog.api.jakarta;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Consumes
@Produces
@Path("api")
@Tag(name = "Nessie REST API proxy")
public interface HttpProxy {
  @HEAD
  @Path("{path:.*}")
  Response doHead(@PathParam("path") String path);

  @GET
  @Path("{path:.*}")
  Response doGet(@PathParam("path") String path);

  @POST
  @Path("{path:.*}")
  Response doPost(@PathParam("path") String path, InputStream data);

  @PUT
  @Path("{path:.*}")
  Response doPut(@PathParam("path") String path, InputStream data);

  @DELETE
  @Path("{path:.*}")
  Response doDelete(@PathParam("path") String path);
}
