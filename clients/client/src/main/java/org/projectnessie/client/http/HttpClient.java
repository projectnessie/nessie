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

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simple Http client to make REST calls.
 *
 * <p>
 *   Assumptions:
 *   - always send/receive JSON
 *   - set headers accordingly by default
 *   - very simple interactions w/ API
 *   - no cookies
 *   - no caching of connections. Could be slow
 * </p>
 */
public class HttpClient {

  private final URI baseUri;
  private final String accept;
  private final ObjectMapper mapper;
  private final SSLContext sslContext;
  private final List<RequestFilter> requestFilters = new ArrayList<>();
  private final List<ResponseFilter> responseFilters = new ArrayList<>();

  public enum Method {
    GET,
    POST,
    PUT,
    DELETE;
  }

  /**
   * Construct an HTTP client with a universal Accept header.
   *
   * @param baseUri uri base eg https://example.com
   * @param accept Accept header eg "application/json"
   */
  private HttpClient(URI baseUri, String accept, ObjectMapper mapper, SSLContext sslContext) {
    this.baseUri = Objects.requireNonNull(baseUri);
    this.accept = HttpUtils.checkNonNullTrim(accept);
    this.mapper = mapper;
    this.sslContext = sslContext;
    if (!"http".equals(baseUri.getScheme()) && !"https".equals(baseUri.getScheme())) {
      throw new IllegalArgumentException(String.format("Cannot start http client. %s must be a valid http or https address", baseUri));
    }
  }

  /**
   * Register a request filter. This filter will be run before the request starts and can modify eg headers.
   */
  public void register(RequestFilter filter) {
    requestFilters.add(filter);
  }

  /**
   * Register a response filter. This filter will be run after the request finishes and can for example handle error states.
   */
  public void register(ResponseFilter filter) {
    responseFilters.add(filter);
  }

  public HttpRequest newRequest() {
    return new HttpRequest(baseUri, accept, mapper, requestFilters, responseFilters, sslContext);
  }

  public static HttpClientBuilder builder() {
    return new HttpClientBuilder();
  }

  public static class HttpClientBuilder {
    private URI baseUri;
    private String accept = "application/json";
    private ObjectMapper mapper;
    private SSLContext sslContext;

    private HttpClientBuilder() {
    }

    public URI getBaseUri() {
      return baseUri;
    }

    public HttpClientBuilder setBaseUri(URI baseUri) {
      this.baseUri = baseUri;
      return this;
    }

    public String getAccept() {
      return accept;
    }

    public HttpClientBuilder setAccept(String accept) {
      this.accept = accept;
      return this;
    }

    public ObjectMapper getMapper() {
      return mapper;
    }

    public HttpClientBuilder setObjectMapper(ObjectMapper mapper) {
      this.mapper = mapper;
      return this;
    }

    public SSLContext getSslContext() {
      return sslContext;
    }

    public HttpClientBuilder setSslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    /**
     * Construct an HttpClient from builder settings.
     */
    public HttpClient build() {
      HttpUtils.checkArgument(baseUri != null, "Cannot construct Http client. Must have a non-null uri");
      HttpUtils.checkArgument(accept != null, "Cannot construct Http client. Must have a non-null accept string");
      HttpUtils.checkArgument(mapper != null, "Cannot construct Http client. Must have a non-null object mapper");
      if (sslContext == null) {
        try {
          sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
          throw new HttpClientException("Cannot construct Http Client. Default SSL config is invalid.", e);
        }
      }
      return new HttpClient(baseUri, accept, mapper, sslContext);
    }
  }
}
