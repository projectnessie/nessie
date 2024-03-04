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

import static org.projectnessie.client.NessieConfigConstants.CONF_ENABLE_API_COMPATIBILITY_CHECK;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.client.rest.v1.HttpApiV1;
import org.projectnessie.client.rest.v1.RestV1Client;
import org.projectnessie.client.rest.v2.HttpApiV2;
import org.projectnessie.model.ser.Views;

/** {@link NessieHttpClientBuilder} and {@link NessieClientBuilder} implementation for HTTP/REST. */
public class NessieHttpClientBuilderImpl
    extends NessieHttpClientBuilder.AbstractNessieHttpClientBuilder {

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(SerializationFeature.INDENT_OUTPUT)
          .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private final HttpClient.Builder builder =
      HttpClient.builder()
          .setObjectMapper(MAPPER)
          .addResponseFilter(new NessieHttpResponseFilter());

  private boolean tracing;

  private boolean enableApiCompatibilityCheck =
      Boolean.parseBoolean(System.getProperty(CONF_ENABLE_API_COMPATIBILITY_CHECK, "true"));

  public NessieHttpClientBuilderImpl() {}

  @Override
  public String name() {
    return "HTTP";
  }

  @Override
  public int priority() {
    return 100;
  }

  /**
   * Set the Nessie server URI. A server URI must be configured.
   *
   * @param uri server URI
   * @return {@code this}
   */
  @Override
  @CanIgnoreReturnValue
  public NessieHttpClientBuilderImpl withUri(URI uri) {
    builder.setBaseUri(uri);
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  public NessieHttpClientBuilderImpl withAuthentication(NessieAuthentication authentication) {
    if (authentication != null && !(authentication instanceof HttpAuthentication)) {
      throw new IllegalArgumentException(
          "HttpClientBuilder only accepts instances of HttpAuthentication");
    }
    builder.setAuthentication((HttpAuthentication) authentication);
    return this;
  }

  /**
   * Whether to enable adding the HTTP headers of an active OpenTracing span to all Nessie requests.
   * If enabled, the OpenTracing dependencies must be present at runtime.
   *
   * @param tracing {@code true} to enable passing HTTP headers for active tracing spans.
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withTracing(boolean tracing) {
    this.tracing = tracing;
    return this;
  }

  /**
   * Set the read timeout in milliseconds for this client. Timeout will throw {@link
   * HttpClientReadTimeoutException}.
   *
   * @param readTimeoutMillis number of seconds to wait for a response from server.
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withReadTimeout(int readTimeoutMillis) {
    builder.setReadTimeoutMillis(readTimeoutMillis);
    return this;
  }

  /**
   * Set the connection timeout in milliseconds for this client. Timeout will throw {@link
   * HttpClientException}.
   *
   * @param connectionTimeoutMillis number of seconds to wait to connect to the server.
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withConnectionTimeout(int connectionTimeoutMillis) {
    builder.setConnectionTimeoutMillis(connectionTimeoutMillis);
    return this;
  }

  /**
   * Set whether the compression shall be disabled or not.
   *
   * @param disableCompression whether the compression shall be disabled or not.
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withDisableCompression(boolean disableCompression) {
    builder.setDisableCompression(disableCompression);
    return this;
  }

  /**
   * Set the SSL context for this client.
   *
   * @param sslContext the SSL context to use
   * @return {@code this}
   */
  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withSSLContext(SSLContext sslContext) {
    builder.setSslContext(sslContext);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withSSLParameters(SSLParameters sslParameters) {
    builder.setSslParameters(sslParameters);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withHttp2Upgrade(boolean http2Upgrade) {
    builder.setHttp2Upgrade(http2Upgrade);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withFollowRedirects(String redirects) {
    builder.setFollowRedirects(redirects);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withForceUrlConnectionClient(
      boolean forceUrlConnectionClient) {
    builder.setForceUrlConnectionClient(forceUrlConnectionClient);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withApiCompatibilityCheck(boolean enable) {
    enableApiCompatibilityCheck = enable;
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilderImpl withResponseFactory(HttpResponseFactory responseFactory) {
    builder.setResponseFactory(responseFactory);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilder addRequestFilter(RequestFilter filter) {
    builder.addRequestFilter(filter);
    return this;
  }

  @CanIgnoreReturnValue
  @Override
  public NessieHttpClientBuilder addResponseFilter(ResponseFilter filter) {
    builder.addResponseFilter(filter);
    return this;
  }

  @Override
  public <API extends NessieApi> API build(Class<API> apiVersion) {
    Objects.requireNonNull(apiVersion, "API version class must be non-null");

    if (tracing) {
      // Do this at the last possible moment because once added, tracing cannot be removed.
      builder.addTracing();
    }

    if (apiVersion.isAssignableFrom(HttpApiV1.class)) {
      if (enableApiCompatibilityCheck) {
        builder.addRequestFilter(new NessieApiCompatibilityFilter(builder, 1));
      }
      builder.setJsonView(Views.V1.class);
      HttpClient httpClient = HttpClients.buildClient(tracing, builder);
      return apiVersion.cast(new HttpApiV1(new RestV1Client(httpClient)));
    }

    if (apiVersion.isAssignableFrom(HttpApiV2.class)) {
      if (enableApiCompatibilityCheck) {
        builder.addRequestFilter(new NessieApiCompatibilityFilter(builder, 2));
      }
      builder.setJsonView(Views.V2.class);
      HttpClient httpClient = HttpClients.buildClient(tracing, builder);
      return apiVersion.cast(new HttpApiV2(httpClient));
    }

    throw new IllegalArgumentException(
        String.format("API version %s is not supported.", apiVersion.getName()));
  }
}
