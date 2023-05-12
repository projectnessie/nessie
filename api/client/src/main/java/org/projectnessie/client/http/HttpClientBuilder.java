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

import static org.projectnessie.client.NessieConfigConstants.CONF_CONNECT_TIMEOUT;
import static org.projectnessie.client.NessieConfigConstants.CONF_ENABLE_API_COMPATIBILITY_CHECK;
import static org.projectnessie.client.NessieConfigConstants.CONF_FORCE_URL_CONNECTION_CLIENT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_DISABLE_COMPRESSION;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_HTTP_2;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_HTTP_REDIRECT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SNI_HOSTS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SNI_MATCHER;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SSL_CIPHER_SUITES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SSL_PROTOCOLS;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_TRACING;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.client.NessieConfigConstants.CONF_READ_TIMEOUT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.NessieAuthenticationProvider;
import org.projectnessie.client.http.v1api.HttpApiV1;
import org.projectnessie.client.http.v2api.HttpApiV2;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.model.ser.Views;

/**
 * A builder class that creates a {@link NessieHttpClient} via {@link HttpClientBuilder#builder()}.
 */
public class HttpClientBuilder implements NessieClientBuilder<HttpClientBuilder> {

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(SerializationFeature.INDENT_OUTPUT)
          .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private final HttpClient.Builder builder =
      HttpClient.builder()
          .setObjectMapper(MAPPER)
          .addResponseFilter(new NessieHttpResponseFilter());

  private boolean tracing;

  private boolean enableApiCompatibilityCheck = true;

  protected HttpClientBuilder() {}

  public static HttpClientBuilder builder() {
    return new HttpClientBuilder();
  }

  /**
   * Same semantics as {@link #fromConfig(Function)}, uses the system properties.
   *
   * @return {@code this}
   * @see #fromConfig(Function)
   */
  @Override
  public HttpClientBuilder fromSystemProperties() {
    return fromConfig(System::getProperty);
  }

  /**
   * Configure this HttpClientBuilder instance using a configuration object and standard Nessie
   * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
   * Non-{@code null} values returned by the {@code configuration}-function will override previously
   * configured values.
   *
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see #fromSystemProperties()
   */
  @Override
  public HttpClientBuilder fromConfig(Function<String, String> configuration) {
    String uri = configuration.apply(CONF_NESSIE_URI);
    if (uri != null) {
      withUri(URI.create(uri));
    }

    withAuthenticationFromConfig(configuration);

    String s = configuration.apply(CONF_NESSIE_TRACING);
    if (s != null) {
      withTracing(Boolean.parseBoolean(s));
    }
    s = configuration.apply(CONF_CONNECT_TIMEOUT);
    if (s != null) {
      withConnectionTimeout(Integer.parseInt(s));
    }
    s = configuration.apply(CONF_READ_TIMEOUT);
    if (s != null) {
      withReadTimeout(Integer.parseInt(s));
    }
    s = configuration.apply(CONF_NESSIE_DISABLE_COMPRESSION);
    if (s != null) {
      withDisableCompression(Boolean.parseBoolean(s));
    }

    SSLParameters sslParameters = new SSLParameters();
    boolean hasSslParameters = false;
    s = configuration.apply(CONF_NESSIE_SSL_CIPHER_SUITES);
    if (s != null) {
      hasSslParameters = true;
      sslParameters.setCipherSuites(
          Arrays.stream(s.split(","))
              .map(String::trim)
              .filter(v -> !v.isEmpty())
              .toArray(String[]::new));
    }
    s = configuration.apply(CONF_NESSIE_SSL_PROTOCOLS);
    if (s != null) {
      hasSslParameters = true;
      sslParameters.setProtocols(
          Arrays.stream(s.split(","))
              .map(String::trim)
              .filter(v -> !v.isEmpty())
              .toArray(String[]::new));
    }
    s = configuration.apply(CONF_NESSIE_SNI_HOSTS);
    if (s != null) {
      hasSslParameters = true;
      sslParameters.setServerNames(
          Arrays.stream(s.split(","))
              .map(String::trim)
              .filter(v -> !v.isEmpty())
              .map(SNIHostName::new)
              .collect(Collectors.toList()));
    }
    s = configuration.apply(CONF_NESSIE_SNI_MATCHER);
    if (s != null) {
      hasSslParameters = true;
      sslParameters.setSNIMatchers(Collections.singletonList(SNIHostName.createSNIMatcher(s)));
    }
    if (hasSslParameters) {
      withSSLParameters(sslParameters);
    }

    s = configuration.apply(CONF_NESSIE_HTTP_2);
    if (s != null) {
      withHttp2Upgrade(Boolean.parseBoolean(s.trim()));
    }

    s = configuration.apply(CONF_NESSIE_HTTP_REDIRECT);
    if (s != null) {
      withFollowRedirects(s.trim());
    }

    s = configuration.apply(CONF_FORCE_URL_CONNECTION_CLIENT);
    if (s != null) {
      withForceUrlConnectionClient(Boolean.parseBoolean(s.trim()));
    }

    s = configuration.apply(CONF_ENABLE_API_COMPATIBILITY_CHECK);
    if (s != null) {
      withEnableApiCompatibilityCheck(Boolean.parseBoolean(s));
    }

    return this;
  }

  /**
   * Configure only authentication in this HttpClientBuilder instance using a configuration object
   * and standard Nessie configuration keys defined by the constants defined in {@link
   * NessieConfigConstants}.
   *
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see #fromConfig(Function)
   */
  @Override
  @CanIgnoreReturnValue
  public HttpClientBuilder withAuthenticationFromConfig(Function<String, String> configuration) {
    withAuthentication(NessieAuthenticationProvider.fromConfig(configuration));
    return this;
  }

  /**
   * Set the Nessie server URI. A server URI must be configured.
   *
   * @param uri server URI
   * @return {@code this}
   */
  @Override
  @CanIgnoreReturnValue
  public HttpClientBuilder withUri(URI uri) {
    builder.setBaseUri(uri);
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  public HttpClientBuilder withAuthentication(NessieAuthentication authentication) {
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
  public HttpClientBuilder withTracing(boolean tracing) {
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
  public HttpClientBuilder withReadTimeout(int readTimeoutMillis) {
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
  public HttpClientBuilder withConnectionTimeout(int connectionTimeoutMillis) {
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
  public HttpClientBuilder withDisableCompression(boolean disableCompression) {
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
  public HttpClientBuilder withSSLContext(SSLContext sslContext) {
    builder.setSslContext(sslContext);
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClientBuilder withSSLParameters(SSLParameters sslParameters) {
    builder.setSslParameters(sslParameters);
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClientBuilder withHttp2Upgrade(boolean http2Upgrade) {
    builder.setHttp2Upgrade(http2Upgrade);
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClientBuilder withFollowRedirects(String redirects) {
    builder.setFollowRedirects(redirects);
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClientBuilder withForceUrlConnectionClient(boolean forceUrlConnectionClient) {
    builder.setForceUrlConnectionClient(forceUrlConnectionClient);
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClientBuilder withEnableApiCompatibilityCheck(boolean enable) {
    enableApiCompatibilityCheck = enable;
    return this;
  }

  @CanIgnoreReturnValue
  public HttpClientBuilder withResponseFactory(HttpResponseFactory responseFactory) {
    builder.setResponseFactory(responseFactory);
    return this;
  }

  @Override
  public <API extends NessieApi> API build(Class<API> apiVersion) {
    Objects.requireNonNull(apiVersion, "API version class must be non-null");

    if (tracing) {
      // Do this at the last possible moment because once added, tracing cannot be removed.
      builder.addTracing();
    }

    API api = buildNessieApi(apiVersion);

    if (enableApiCompatibilityCheck) {
      NessieApiCompatibility.checkApiCompatibility(api);
    }

    return api;
  }

  private <API extends NessieApi> API buildNessieApi(Class<API> apiVersion) {

    if (apiVersion.isAssignableFrom(HttpApiV1.class)) {
      builder.setJsonView(Views.V1.class);
      HttpClient httpClient = builder.build();
      @SuppressWarnings("unchecked")
      API api = (API) new HttpApiV1(new NessieHttpClient(httpClient));
      return api;
    }

    if (apiVersion.isAssignableFrom(HttpApiV2.class)) {
      builder.setJsonView(Views.V2.class);
      HttpClient httpClient = builder.build();
      @SuppressWarnings("unchecked")
      API api = (API) new HttpApiV2(httpClient);
      return api;
    }

    throw new IllegalArgumentException(
        String.format("API version %s is not supported.", apiVersion.getName()));
  }
}
