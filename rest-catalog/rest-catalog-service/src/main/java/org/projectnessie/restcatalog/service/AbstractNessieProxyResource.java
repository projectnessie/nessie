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
package org.projectnessie.restcatalog.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.inject.Inject;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;
import org.projectnessie.restcatalog.service.resources.HttpClientHolder;

public abstract class AbstractNessieProxyResource {

  protected static final List<String> REQUEST_HEADERS =
      Arrays.asList(
          "Content-Type",
          "Content-Encoding",
          "Accept",
          "Accept-Encoding",
          "Accept-Language",
          "Nessie-Client-Spec");
  protected static final List<String> RESPONSE_HEADERS =
      Arrays.asList(
          "Content-Type",
          "Content-Length",
          "Content-Encoding",
          "Accept",
          "Expires",
          "Cache-Control",
          "Last-Modified",
          "ETag",
          "Vary");

  @Inject @jakarta.inject.Inject protected TenantSpecific tenantSpecific;
  @Inject @jakarta.inject.Inject protected HttpClientHolder httpClientHolder;

  protected abstract URI baseUri();

  protected abstract URI requestUri();

  protected abstract String requestMethod();

  protected abstract String headerString(String header);

  protected HttpResponse<InputStream> proxyRequest(InputStream data)
      throws IOException, InterruptedException {
    URI target = proxyTo();

    HttpClient httpClient = httpClientHolder.httpClient();

    HttpRequest.BodyPublisher bodyPublisher =
        data != null
            ? HttpRequest.BodyPublishers.ofInputStream(() -> data)
            : HttpRequest.BodyPublishers.noBody();

    HttpRequest.Builder httpRequest =
        HttpRequest.newBuilder(target)
            .timeout(httpClientHolder.requestTimeout())
            .method(requestMethod(), bodyPublisher);
    requestHeaders(httpRequest::header);

    HttpResponse.BodyHandler<InputStream> bodyHandler = HttpResponse.BodyHandlers.ofInputStream();

    return httpClient.send(httpRequest.build(), bodyHandler);
  }

  private URI proxyTo() {
    // Need the request URI, which has everything we need - the path and query parameters
    URI reqUri = requestUri();
    // "Our" base-URI is the one for this resource
    URI baseUri = baseUri();

    // Relativize the request URI
    URI relativeUri = baseUri.relativize(reqUri);

    URI nessieApiBaseUri = tenantSpecific.nessieApiBaseUri();

    // Ensure that the resolved URI is not used to abuse this proxy for anything else than Nessie
    // Core REST API calls. Otherwise, it could be used to issue generic requests to other resources
    // on the same network.
    if (relativeUri.isAbsolute()) {
      // Status message isn't always propagated into the result
      throw new ClientErrorException(Response.Status.BAD_REQUEST);
    }

    // Resolve the relative request URI against the Nessie Core server base URI. This gives us the
    // properly encoded path and query parameters.
    return nessieApiBaseUri.resolve(relativeUri);
  }

  protected void requestHeaders(BiConsumer<String, String> headerConsumer) {
    for (String hdr : REQUEST_HEADERS) {
      String value = headerString(hdr);
      if (value != null) {
        headerConsumer.accept(hdr, value);
      }
    }
  }

  protected void responseHeaders(
      Function<String, List<String>> headerProvider, BiConsumer<String, String> headerConsumer) {
    for (String hdr : RESPONSE_HEADERS) {
      List<String> value = headerProvider.apply(hdr);
      if (value != null) {
        value.forEach(v -> headerConsumer.accept(hdr, v));
      }
    }
  }
}
