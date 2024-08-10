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
package org.projectnessie.api.v1.http;

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.api.v1.ConfigApi;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;

@Tag(name = "v1")
@Path("v1/config")
public interface HttpConfigApi extends ConfigApi {

  @Override
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all configuration settings")
  @APIResponses({
    @APIResponse(
        description = "Configuration settings",
        content =
            @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = NessieConfiguration.class),
                examples = {@ExampleObject(ref = "nessieConfigV1")})),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "400", description = "Unknown Error")
  })
  @JsonView(Views.V1.class)
  NessieConfiguration getConfig();
}
