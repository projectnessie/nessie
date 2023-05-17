/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v2.http;

import com.fasterxml.jackson.annotation.JsonView;
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
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.api.v2.ConfigApi;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;

@Path("v2/config")
@jakarta.ws.rs.Path("v2/config")
@Tag(name = "v2")
public interface HttpConfigApi extends ConfigApi {

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Returns repository and server settings relevant to clients.",
      operationId = "getConfigV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Configuration settings",
        content =
            @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = NessieConfiguration.class),
                examples = {@ExampleObject(ref = "nessieConfig")})),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided")
  })
  @JsonView(Views.V2.class)
  NessieConfiguration getConfig();
}
