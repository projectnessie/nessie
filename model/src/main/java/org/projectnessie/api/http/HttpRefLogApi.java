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
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.projectnessie.api.RefLogApi;
import org.projectnessie.api.params.RefLogParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("reflogs")
public interface HttpRefLogApi extends RefLogApi {

  @Override
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get reflog entries",
      description =
          "Retrieve the reflog entries from a specified endHash or from the current HEAD if the endHash is null, "
              + "potentially truncated by the backend.\n"
              + "\n"
              + "Retrieves up to 'maxRecords' refLog-entries starting at the endHash or HEAD."
              + "The backend may respect the given 'max' records hint, but return less or more entries. "
              + "Backends may also cap the returned entries at a hard-coded limit, the default "
              + "REST server implementation has such a hard-coded limit.\n"
              + "\n"
              + "To implement paging, check 'hasMore' in the response and, if 'true', pass the value "
              + "returned as 'token' in the next invocation as the 'pageToken' parameter.\n"
              + "\n"
              + "The content and meaning of the returned 'token' is \"private\" to the implementation,"
              + "treat is as an opaque value.\n"
              + "\n"
              + "It is wrong to assume that invoking this method with a very high 'maxRecords' value "
              + "will return all reflog entries.\n"
              + "\n")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned reflog entries.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema = @Schema(implementation = RefLogResponse.class))
        }),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "400", description = "Unknown Error"),
    @APIResponse(responseCode = "404", description = "Reflog id doesn't exists")
  })
  RefLogResponse getRefLog(@BeanParam RefLogParams params) throws NessieNotFoundException;
}
