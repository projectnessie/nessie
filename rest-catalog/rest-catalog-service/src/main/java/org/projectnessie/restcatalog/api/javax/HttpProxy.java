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
package org.projectnessie.restcatalog.api.javax;

import java.io.InputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
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
