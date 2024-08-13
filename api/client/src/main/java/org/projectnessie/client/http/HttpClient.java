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
package org.projectnessie.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

/**
 * Simple Http client to make REST calls.
 *
 * <p>Assumptions: - always send/receive JSON - set headers accordingly by default - very simple
 * interactions w/ API - no cookies - no caching of connections. Could be slow
 */
public interface HttpClient extends AutoCloseable {

  enum Method {
    GET,
    POST,
    PUT,
    DELETE,
  }

  default HttpRequest newRequest() {
    return newRequest(getBaseUri());
  }

  HttpRequest newRequest(URI baseUri);

  static Builder builder() {
    return new HttpClientBuilderImpl();
  }

  URI getBaseUri();

  @Override
  void close();

  interface Builder {
    @SuppressWarnings("unused")
    @CanIgnoreReturnValue
    Builder setClientSpec(int clientSpec);

    @CanIgnoreReturnValue
    Builder setBaseUri(URI baseUri);

    @CanIgnoreReturnValue
    Builder setDisableCompression(boolean disableCompression);

    @CanIgnoreReturnValue
    Builder setObjectMapper(ObjectMapper mapper);

    @CanIgnoreReturnValue
    Builder setJsonView(Class<?> jsonView);

    @CanIgnoreReturnValue
    Builder setResponseFactory(HttpResponseFactory responseFactory);

    @CanIgnoreReturnValue
    Builder setSslNoCertificateVerification(boolean noCertificateVerification);

    @CanIgnoreReturnValue
    Builder setSslContext(SSLContext sslContext);

    @CanIgnoreReturnValue
    Builder setSslParameters(SSLParameters sslParameters);

    @CanIgnoreReturnValue
    Builder setAuthentication(HttpAuthentication authentication);

    @CanIgnoreReturnValue
    Builder setHttp2Upgrade(boolean http2Upgrade);

    @CanIgnoreReturnValue
    Builder setFollowRedirects(String followRedirects);

    @SuppressWarnings("DeprecatedIsStillUsed")
    @CanIgnoreReturnValue
    @Deprecated
    Builder setForceUrlConnectionClient(boolean forceUrlConnectionClient);

    @CanIgnoreReturnValue
    Builder setHttpClientName(String clientName);

    @CanIgnoreReturnValue
    Builder setReadTimeoutMillis(int readTimeoutMillis);

    @CanIgnoreReturnValue
    Builder setConnectionTimeoutMillis(int connectionTimeoutMillis);

    /**
     * Register a request filter. This filter will be run before the request starts and can modify
     * eg headers.
     */
    @CanIgnoreReturnValue
    Builder addRequestFilter(RequestFilter filter);

    /**
     * Register a response filter. This filter will be run after the request finishes and can for
     * example handle error states.
     */
    @CanIgnoreReturnValue
    Builder addResponseFilter(ResponseFilter filter);

    /**
     * Add tracing to the client. This will load the opentracing libraries. It is not possible to
     * remove tracing once it is added.
     */
    @CanIgnoreReturnValue
    Builder addTracing();

    @CanIgnoreReturnValue
    Builder setCancellationFuture(CompletionStage<?> cancellationFuture);

    @CanIgnoreReturnValue
    Builder addCustomHeader(String header, String value);

    /** Construct an HttpClient from builder settings. */
    HttpClient build();
  }
}
