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

package com.dremio.nessie.client;

import com.dremio.nessie.client.rest.ConflictException;
import com.dremio.nessie.client.rest.PreconditionFailedException;
import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.dremio.nessie.model.Base;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * common REST utils.
 */
public final class RestUtils {

  private RestUtils() {

  }

  static void checkResponse(Response response) {
    if (response.getStatus() == Status.OK.getStatusCode()
        || response.getStatus() == Status.CREATED.getStatusCode()) {
      return;
    }
    if (response.getStatus() == Status.NOT_FOUND.getStatusCode()) {
      throw new NotFoundException(response);
    }
    if (response.getStatus() == Status.FORBIDDEN.getStatusCode()) {
      throw new ForbiddenException(response);
    }
    if (response.getStatus() == Status.UNAUTHORIZED.getStatusCode()) {
      throw new NotAuthorizedException(response);
    }
    if (response.getStatus() == Status.BAD_REQUEST.getStatusCode()) {
      throw new BadRequestException(response);
    }
    if (response.getStatus() == Status.PRECONDITION_FAILED.getStatusCode()) {
      throw new PreconditionFailedException(response);
    }
    if (response.getStatus() == Status.CONFLICT.getStatusCode()) {
      throw new ConflictException(response);
    }
    throw new RuntimeException(
      "Unknown exception " + response.getStatus() + " " + response.readEntity(String.class));
  }

  static class ClientWithHelpers {

    private final Client client;

    ClientWithHelpers() {
      this.client = ClientBuilder.newBuilder().register(ObjectMapperContextResolver.class).build();
    }

    public void close() {
      client.close();
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType,
                                  String authHeader) {
      Map<String, String> params = ImmutableMap.of();
      return get(endpoint, path, mediaType, authHeader, params);
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType, String authHeader,
                                  Map<String, String> queryParams) {
      WebTarget webTarget = client.target(endpoint);
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
      Invocation.Builder builder = webTarget.path(path)
                                            .request(mediaType)
                                            .accept(MediaType.APPLICATION_JSON_TYPE);
      if (authHeader != null) {
        builder.header(HttpHeaders.AUTHORIZATION, authHeader);
      }
      return builder;
    }
  }
}
