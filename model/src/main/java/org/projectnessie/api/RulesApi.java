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
package org.projectnessie.api;

import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.AuthorizationRule;

@Path("rules")
public interface RulesApi {

  @POST
  @Path("rule")
  @Operation(summary = "Add a new authorization rule")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Created successfully.",
        content = {@Content(examples = {@ExampleObject(ref = "authRuleObj")})}),
    @APIResponse(responseCode = "409", description = "Authorization rule already exists")
  })
  void addRule(
      @Valid
          @NotNull
          @RequestBody(
              description = "Authorization rule to create.",
              content = {@Content(examples = {@ExampleObject(ref = "authRuleObj")})})
          AuthorizationRule rule)
      throws NessieConflictException;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  Set<AuthorizationRule> getRules();

  @DELETE
  @Path("rule/{id}")
  @APIResponses({
    @APIResponse(responseCode = "200", description = "Deleted successfully."),
    @APIResponse(responseCode = "409", description = "Authorization rule does not exist")
  })
  void deleteRule(@PathParam("id") String id) throws NessieConflictException;
}
