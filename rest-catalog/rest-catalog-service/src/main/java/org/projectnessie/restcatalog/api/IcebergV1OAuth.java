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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.restcatalog.api.errors.OAuthTokenEndpointException;
import org.projectnessie.restcatalog.service.auth.OAuthTokenRequest;

/** Iceberg OAuth token endpoint. */
@Path("iceberg/v1")
@jakarta.ws.rs.Path("iceberg/v1")
@Tag(name = "Iceberg v1")
public interface IcebergV1OAuth {

  // javax
  @POST
  @Path("/oauth/tokens")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  // jakarta
  @jakarta.ws.rs.POST
  @jakarta.ws.rs.Path("/oauth/tokens")
  @jakarta.ws.rs.Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  OAuthTokenResponse getToken(OAuthTokenRequest request) throws OAuthTokenEndpointException;
}
