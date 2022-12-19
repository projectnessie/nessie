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
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.HttpUtils;
import org.projectnessie.client.http.impl.jdk11.JavaHttpClient;
import org.projectnessie.client.http.impl.jdk8.UrlConnectionClient;

/**
 * Simple Http client to make REST calls.
 *
 * <p>Assumptions: - always send/receive JSON - set headers accordingly by default - very simple
 * interactions w/ API - no cookies - no caching of connections. Could be slow
 */
public interface HttpClient {

  enum Method {
    GET,
    POST,
    PUT,
    DELETE;
  }

  HttpRequest newRequest();

  static Builder builder() {
    return new Builder();
  }

  URI getBaseUri();

  class Builder {
    private URI baseUri;
    private ObjectMapper mapper;
    private Class<?> jsonView;
    private SSLContext sslContext;
    private SSLParameters sslParameters;
    private int readTimeoutMillis =
        Integer.parseInt(System.getProperty("sun.net.client.defaultReadTimeout", "25000"));
    private int connectionTimeoutMillis =
        Integer.parseInt(System.getProperty("sun.net.client.defaultConnectionTimeout", "5000"));
    private boolean disableCompression;
    private final List<RequestFilter> requestFilters = new ArrayList<>();
    private final List<ResponseFilter> responseFilters = new ArrayList<>();
    private boolean http2Upgrade;
    private String followRedirects;
    private boolean forceUrlConnectionClient;

    private Builder() {}

    public URI getBaseUri() {
      return baseUri;
    }

    public Builder setBaseUri(URI baseUri) {
      this.baseUri = baseUri;
      return this;
    }

    public Builder setDisableCompression(boolean disableCompression) {
      this.disableCompression = disableCompression;
      return this;
    }

    public Builder setObjectMapper(ObjectMapper mapper) {
      this.mapper = mapper;
      return this;
    }

    public Builder setJsonView(Class<?> jsonView) {
      this.jsonView = jsonView;
      return this;
    }

    public Builder setSslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    public Builder setSslParameters(SSLParameters sslParameters) {
      this.sslParameters = sslParameters;
      return this;
    }

    public Builder setHttp2Upgrade(boolean http2Upgrade) {
      this.http2Upgrade = http2Upgrade;
      return this;
    }

    public Builder setFollowRedirects(String followRedirects) {
      this.followRedirects = followRedirects;
      return this;
    }

    public Builder setForceUrlConnectionClient(boolean forceUrlConnectionClient) {
      this.forceUrlConnectionClient = forceUrlConnectionClient;
      return this;
    }

    public Builder setReadTimeoutMillis(int readTimeoutMillis) {
      this.readTimeoutMillis = readTimeoutMillis;
      return this;
    }

    public Builder setConnectionTimeoutMillis(int connectionTimeoutMillis) {
      this.connectionTimeoutMillis = connectionTimeoutMillis;
      return this;
    }

    /**
     * Register a request filter. This filter will be run before the request starts and can modify
     * eg headers.
     */
    public Builder addRequestFilter(RequestFilter filter) {
      requestFilters.add(filter);
      return this;
    }

    /**
     * Register a response filter. This filter will be run after the request finishes and can for
     * example handle error states.
     */
    public Builder addResponseFilter(ResponseFilter filter) {
      responseFilters.add(filter);
      return this;
    }

    /** Construct an HttpClient from builder settings. */
    public HttpClient build() {
      HttpUtils.checkArgument(
          baseUri != null, "Cannot construct Http client. Must have a non-null uri");
      HttpUtils.checkArgument(
          mapper != null, "Cannot construct Http client. Must have a non-null object mapper");
      if (sslContext == null) {
        try {
          sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
          throw new HttpClientException(
              "Cannot construct Http Client. Default SSL config is invalid.", e);
        }
      }

      HttpRuntimeConfig config =
          HttpRuntimeConfig.builder()
              .baseUri(baseUri)
              .mapper(mapper)
              .jsonView(jsonView)
              .readTimeoutMillis(readTimeoutMillis)
              .connectionTimeoutMillis(connectionTimeoutMillis)
              .isDisableCompression(disableCompression)
              .sslContext(sslContext)
              .sslParameters(sslParameters)
              .addAllRequestFilters(requestFilters)
              .addAllResponseFilters(responseFilters)
              .isHttp11Only(!http2Upgrade)
              .followRedirects(followRedirects)
              .forceUrlConnectionClient(forceUrlConnectionClient)
              .build();

      return ImplSwitch.FACTORY.apply(config);
    }

    static class ImplSwitch {
      static final Function<HttpRuntimeConfig, HttpClient> FACTORY;

      static {
        Function<HttpRuntimeConfig, HttpClient> factory;
        try {
          Class.forName("java.net.http.HttpClient");
          factory =
              config ->
                  // Need the system property for tests, "normal" users can use standard
                  // configuration options.
                  Boolean.getBoolean("nessie.client.force-url-connection-client")
                          || config.forceUrlConnectionClient()
                      ? new UrlConnectionClient(config)
                      : new JavaHttpClient(config);
        } catch (ClassNotFoundException e) {
          factory = UrlConnectionClient::new;
        }
        FACTORY = factory;
      }
    }
  }
}
