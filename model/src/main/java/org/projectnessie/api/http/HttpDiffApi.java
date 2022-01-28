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

import javax.ws.rs.BeanParam;
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
import org.projectnessie.api.DiffApi;
import org.projectnessie.api.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("diffs")
public interface HttpDiffApi extends DiffApi {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path(
      "{fromRef : [^*]+}{f : [*]?}{fromHashOnRef : ([^.]*)?}...{toRef : [^*]+}{t : [*]?}{toHashOnRef : ([^.]*)?}")
  @Operation(
      summary = "Get a diff for two given references",
      description =
          "The URL pattern is basically 'from' and 'to' separated by '...' (three dots). "
              + "'from' and 'to' must start with a reference name, optionally followed by hash on "
              + "that reference, the hash prefixed with the'*' character.\n"
              + "\n"
              + "Examples: \n"
              + "  diffs/main...myBranch\n"
              + "  diffs/main...myBranch*1234567890123456\n"
              + "  diffs/main*1234567890123456...myBranch\n"
              + "  diffs/main*1234567890123456...myBranch*1234567890123456\n")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned diff for the given references.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "diffResponse"),
                },
                schema = @Schema(implementation = DiffResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, fromRef/toRef name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view the given fromRef/toRef"),
    @APIResponse(responseCode = "404", description = "fromRef/toRef not found"),
  })
  DiffResponse getDiff(@BeanParam DiffParams params) throws NessieNotFoundException;
}
