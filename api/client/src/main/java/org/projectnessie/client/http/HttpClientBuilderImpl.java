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
package org.projectnessie.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.HttpUtils;
import org.projectnessie.client.http.impl.jdk11.JavaHttpClient;
import org.projectnessie.client.http.impl.jdk8.UrlConnectionClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HttpClientBuilderImpl implements HttpClient.Builder {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientBuilderImpl.class);

  private URI baseUri;
  private ObjectMapper mapper;
  private Class<?> jsonView;
  private HttpResponseFactory responseFactory = HttpResponse::new;
  private SSLContext sslContext;
  private SSLParameters sslParameters;
  private HttpAuthentication authentication;
  private int readTimeoutMillis =
      Integer.getInteger(
          "sun.net.client.defaultReadTimeout", NessieConfigConstants.DEFAULT_READ_TIMEOUT_MILLIS);
  private int connectionTimeoutMillis =
      Integer.getInteger(
          "sun.net.client.defaultConnectionTimeout",
          NessieConfigConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
  private boolean disableCompression;
  private final List<RequestFilter> requestFilters = new ArrayList<>();
  private final List<ResponseFilter> responseFilters = new ArrayList<>();
  private boolean http2Upgrade;
  private String followRedirects;
  private boolean forceUrlConnectionClient;
  private int clientSpec = 2;

  HttpClientBuilderImpl() {}

  HttpClientBuilderImpl(HttpClientBuilderImpl other) {
    this.baseUri = other.baseUri;
    this.mapper = other.mapper;
    this.jsonView = other.jsonView;
    this.responseFactory = other.responseFactory;
    this.sslContext = other.sslContext;
    this.sslParameters = other.sslParameters;
    this.authentication = other.authentication;
    this.readTimeoutMillis = other.readTimeoutMillis;
    this.connectionTimeoutMillis = other.connectionTimeoutMillis;
    this.disableCompression = other.disableCompression;
    this.requestFilters.addAll(other.requestFilters);
    this.responseFilters.addAll(other.responseFilters);
    this.http2Upgrade = other.http2Upgrade;
    this.followRedirects = other.followRedirects;
    this.forceUrlConnectionClient = other.forceUrlConnectionClient;
    this.clientSpec = other.clientSpec;
  }

  /** Creates a (shallow) copy of this builder. */
  @Override
  public HttpClient.Builder copy() {
    return new HttpClientBuilderImpl(this);
  }

  @CanIgnoreReturnValue
  public HttpClient.Builder setClientSpec(int clientSpec) {
    this.clientSpec = clientSpec;
    return this;
  }

  @Override
  public HttpClient.Builder setBaseUri(URI baseUri) {
    this.baseUri = baseUri;
    return this;
  }

  @Override
  public HttpClient.Builder setDisableCompression(boolean disableCompression) {
    this.disableCompression = disableCompression;
    return this;
  }

  @Override
  public HttpClient.Builder setObjectMapper(ObjectMapper mapper) {
    this.mapper = mapper;
    return this;
  }

  @Override
  public HttpClient.Builder setJsonView(Class<?> jsonView) {
    this.jsonView = jsonView;
    return this;
  }

  @Override
  public HttpClient.Builder setResponseFactory(HttpResponseFactory responseFactory) {
    this.responseFactory = responseFactory;
    return this;
  }

  @Override
  public HttpClient.Builder setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  @Override
  public HttpClient.Builder setSslParameters(SSLParameters sslParameters) {
    this.sslParameters = sslParameters;
    return this;
  }

  @Override
  public HttpClient.Builder setAuthentication(HttpAuthentication authentication) {
    this.authentication = authentication;
    return this;
  }

  @Override
  public HttpClient.Builder setHttp2Upgrade(boolean http2Upgrade) {
    this.http2Upgrade = http2Upgrade;
    return this;
  }

  @Override
  public HttpClient.Builder setFollowRedirects(String followRedirects) {
    this.followRedirects = followRedirects;
    return this;
  }

  @Override
  public HttpClient.Builder setForceUrlConnectionClient(boolean forceUrlConnectionClient) {
    this.forceUrlConnectionClient = forceUrlConnectionClient;
    return this;
  }

  @Override
  public HttpClient.Builder setReadTimeoutMillis(int readTimeoutMillis) {
    this.readTimeoutMillis = readTimeoutMillis;
    return this;
  }

  @Override
  public HttpClient.Builder setConnectionTimeoutMillis(int connectionTimeoutMillis) {
    this.connectionTimeoutMillis = connectionTimeoutMillis;
    return this;
  }

  @Override
  public HttpClient.Builder addRequestFilter(RequestFilter filter) {
    requestFilters.add(filter);
    return this;
  }

  @Override
  public HttpClient.Builder addResponseFilter(ResponseFilter filter) {
    responseFilters.add(filter);
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClient.Builder clearRequestFilters() {
    requestFilters.clear();
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClient.Builder clearResponseFilters() {
    responseFilters.clear();
    return this;
  }

  /**
   * Add tracing to the client. This will load the opentracing libraries. It is not possible to
   * remove tracing once it is added.
   */
  @CanIgnoreReturnValue
  public HttpClient.Builder addTracing() {
    try {
      OpentelemetryTracing.addTracing(this);
    } catch (NoClassDefFoundError e) {
      LOGGER.warn(
          "Failed to initialize tracing, the opentracing libraries are probably missing.", e);
    }
    return this;
  }

  @Override
  public HttpClient build() {
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

    if (authentication != null) {
      authentication.applyToHttpClient(this);
      authentication.start();
    }

    HttpRuntimeConfig config =
        HttpRuntimeConfig.builder()
            .baseUri(baseUri)
            .mapper(mapper)
            .jsonView(jsonView)
            .responseFactory(responseFactory)
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
            .authentication(authentication)
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
