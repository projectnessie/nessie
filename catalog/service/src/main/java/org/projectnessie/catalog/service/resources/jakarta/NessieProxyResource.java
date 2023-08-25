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
package org.projectnessie.catalog.service.resources.jakarta;

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpResponse;
import org.projectnessie.catalog.api.jakarta.HttpProxy;
import org.projectnessie.catalog.service.spi.AbstractNessieProxyResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Jakarta EE ({@code jakarta.*}) specific proxy resource. */
@RequestScoped
public class NessieProxyResource extends AbstractNessieProxyResource implements HttpProxy {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieProxyResource.class);

  public Response doProxy(InputStream data) {
    try {
      HttpResponse<InputStream> httpResponse = proxyRequest(data);

      ResponseBuilder responseBuilder = Response.status(httpResponse.statusCode());
      responseHeaders(httpResponse.headers()::allValues, responseBuilder::header);

      return responseBuilder.entity(httpResponse.body()).build();
    } catch (WebApplicationException e) {
      return e.getResponse();
    } catch (Exception e) {
      LOGGER.error(
          "Internal error during proxy of {} {}",
          restAbstraction.requestMethod(),
          restAbstraction.requestUri(),
          e);
      return new ServerErrorException("Internal error", Response.Status.INTERNAL_SERVER_ERROR)
          .getResponse();
    }
  }

  @Override
  protected void validateRelativeUri(URI relativeUri) {
    if (relativeUri.isAbsolute()) {
      // Status message isn't always propagated into the result
      throw new ClientErrorException(Response.Status.BAD_REQUEST);
    }
  }

  @Override
  public Response doHead(String path) {
    return doProxy(null);
  }

  @Override
  public Response doGet(String path) {
    return doProxy(null);
  }

  @Override
  public Response doPost(String path, InputStream data) {
    return doProxy(data);
  }

  @Override
  public Response doPut(String path, InputStream data) {
    return doProxy(data);
  }

  @Override
  public Response doDelete(String path) {
    return doProxy(null);
  }
}
