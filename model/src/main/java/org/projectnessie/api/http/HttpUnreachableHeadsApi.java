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
package org.projectnessie.api.http;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.projectnessie.api.UnreachableHeadsApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.UnreachableHeadsResponse;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("unreachableHeads")
public interface HttpUnreachableHeadsApi extends UnreachableHeadsApi {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Retrieve all the Unreachable reference heads",
      description =
          "Scans all the commit logs and identifies the unreachable reference heads created due to drop reference or assign reference operation.")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned all the Unreachable reference heads.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "unreachableHeadsResponse"),
                },
                schema = @Schema(implementation = UnreachableHeadsResponse.class))),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view the unreachable heads"),
    @APIResponse(responseCode = "404", description = "Reference doesn't exists"),
  })
  @Override
  UnreachableHeadsResponse getUnreachableReferenceHeads() throws NessieNotFoundException;
}
