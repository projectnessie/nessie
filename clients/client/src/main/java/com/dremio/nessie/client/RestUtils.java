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

  /**
   * check that response had a valid return code. Throw exception if not.
   */
  public static void checkResponse(Response response) {
    Status status = Status.fromStatusCode(response.getStatus());
    switch (status) {

      case OK:
      case CREATED:
        return;
      case BAD_REQUEST:
        throw new BadRequestException(response);
      case UNAUTHORIZED:
        throw new NotAuthorizedException(response);
      case FORBIDDEN:
        throw new ForbiddenException(response);
      case NOT_FOUND:
        throw new NotFoundException(response);
      case CONFLICT:
        throw new ConflictException(response);
      case PRECONDITION_FAILED:
        throw new PreconditionFailedException(response);
      default:
        throw new RuntimeException(
          "Unknown exception " + response.getStatus() + " " + response.readEntity(String.class));
    }
  }

  /**
   * Helper to interact with Jersey client.
   */
  public static class ClientWithHelpers {

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

    /**
     * build http request with given auth header, media type and query params.
     */
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
