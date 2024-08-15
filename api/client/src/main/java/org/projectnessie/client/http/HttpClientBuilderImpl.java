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

import static java.util.Locale.ROOT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_SSL_NO_CERTIFICATE_VERIFICATION;
import static org.projectnessie.client.http.impl.HttpUtils.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.Socket;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.impl.HttpClientFactory;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HttpClientBuilderImpl implements HttpClient.Builder {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientBuilderImpl.class);

  private URI baseUri;
  private ObjectMapper mapper;
  private Class<?> jsonView;
  private HttpResponseFactory responseFactory = HttpResponse::new;
  private boolean sslNoCertificateVerification;
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
  private String httpClientName;
  private int clientSpec = 2;
  private CompletionStage<?> cancellationFuture;
  private final Map<String, List<String>> customHeaders = new HashMap<>();

  HttpClientBuilderImpl() {}

  HttpClientBuilderImpl(HttpClientBuilderImpl other) {
    this.baseUri = other.baseUri;
    this.mapper = other.mapper == null ? null : other.mapper.copy();
    this.jsonView = other.jsonView;
    this.responseFactory = other.responseFactory;
    this.sslNoCertificateVerification = other.sslNoCertificateVerification;
    this.sslContext = other.sslContext;
    this.sslParameters = other.sslParameters;
    this.authentication = other.authentication == null ? null : other.authentication.copy();
    this.readTimeoutMillis = other.readTimeoutMillis;
    this.connectionTimeoutMillis = other.connectionTimeoutMillis;
    this.disableCompression = other.disableCompression;
    this.requestFilters.addAll(other.requestFilters);
    this.responseFilters.addAll(other.responseFilters);
    this.http2Upgrade = other.http2Upgrade;
    this.followRedirects = other.followRedirects;
    this.forceUrlConnectionClient = other.forceUrlConnectionClient;
    this.httpClientName = other.httpClientName;
    this.clientSpec = other.clientSpec;
    this.cancellationFuture = other.cancellationFuture;
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
  public HttpClient.Builder setSslNoCertificateVerification(boolean noCertificateVerification) {
    this.sslNoCertificateVerification = noCertificateVerification;
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
  @Deprecated
  public HttpClient.Builder setForceUrlConnectionClient(boolean forceUrlConnectionClient) {
    this.forceUrlConnectionClient = forceUrlConnectionClient;
    return this;
  }

  @Override
  public HttpClient.Builder setHttpClientName(String httpClientName) {
    this.httpClientName = httpClientName;
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
  public HttpClient.Builder setCancellationFuture(CompletionStage<?> cancellationFuture) {
    this.cancellationFuture = cancellationFuture;
    return this;
  }

  @Override
  public HttpClient.Builder addCustomHeader(String header, String value) {
    customHeaders.computeIfAbsent(header, h -> new ArrayList<>()).add(value);
    return this;
  }

  @Override
  public HttpClient build() {
    checkArgument(
        mapper != null, "Cannot construct Http client. Must have a non-null object mapper");

    SSLContext sslCtx = this.sslContext;

    if (sslNoCertificateVerification) {
      checkArgument(
          sslCtx == null,
          "Cannot construct Http client, must not combine %s and an explicitly configured SSLContext",
          CONF_NESSIE_SSL_NO_CERTIFICATE_VERIFICATION);
      try {
        sslCtx = SSLContext.getInstance("TLS");
        TrustManager trustManager =
            new X509ExtendedTrustManager() {
              @Override
              public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[] {};
              }

              @Override
              public void checkClientTrusted(X509Certificate[] chain, String authType) {}

              @Override
              public void checkServerTrusted(X509Certificate[] chain, String authType) {}

              @Override
              public void checkClientTrusted(
                  X509Certificate[] chain, String authType, Socket socket) {}

              @Override
              public void checkServerTrusted(
                  X509Certificate[] chain, String authType, Socket socket) {}

              @Override
              public void checkClientTrusted(
                  X509Certificate[] chain, String authType, SSLEngine engine) {}

              @Override
              public void checkServerTrusted(
                  X509Certificate[] chain, String authType, SSLEngine engine) {}
            };
        sslCtx.init(null, new TrustManager[] {trustManager}, new SecureRandom());
      } catch (KeyManagementException | NoSuchAlgorithmException e) {
        throw new HttpClientException(
            "Cannot construct Http Client, unable to configure noop trust-manager", e);
      }
    }

    if (sslCtx == null) {
      try {
        sslCtx = SSLContext.getDefault();
      } catch (NoSuchAlgorithmException e) {
        throw new HttpClientException(
            "Cannot construct Http Client. Default SSL config is invalid.", e);
      }
    }

    if (authentication != null) {
      authentication.applyToHttpClient(this);
    }

    if (!customHeaders.isEmpty()) {
      requestFilters.add(
          context ->
              customHeaders.forEach(
                  (header, values) -> values.forEach(v -> context.putHeader(header, v))));
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
            .sslContext(sslCtx)
            .sslParameters(sslParameters)
            .addAllRequestFilters(requestFilters)
            .addAllResponseFilters(responseFilters)
            .isHttp11Only(!http2Upgrade)
            .followRedirects(followRedirects)
            .authentication(authentication)
            .build();

    String clientName = httpClientName;
    // "HTTP" is the name for `org.projectnessie.client.http.NessieHttpClientBuilderImpl`
    if ("HTTP".equalsIgnoreCase(clientName)) {
      // fall back to default
      clientName = null;
    }

    if (forceUrlConnectionClient) {
      if (clientName != null && !"URLConnection".equalsIgnoreCase(clientName)) {
        LOGGER.warn(
            "Both forceUrlConnectionClient and httpClientName are specified with incompatible values, migrate to httpClientName");
      }
      // forced use of URLConnectionClient
      clientName = "URLConnection";
    }

    HttpClientFactory httpClientFactory =
        clientName != null
            ? ImplSwitch.IMPLEMENTATIONS.get(clientName.toLowerCase(ROOT))
            : ImplSwitch.PREFERRED;
    if (httpClientFactory == null) {
      throw new IllegalArgumentException(
          "No HTTP client factory for name '" + clientName + "' found");
    }
    HttpClient client = httpClientFactory.buildClient(config);
    if (cancellationFuture != null) {
      cancellationFuture.thenRun(client::close);
    }

    if (authentication != null) {
      try {
        authentication.start();
      } catch (RuntimeException e) {
        try {
          client.close();
        } catch (RuntimeException e2) {
          e.addSuppressed(e2);
        }
        throw e;
      }
    }

    return client;
  }

  static Set<String> clientNames() {
    return ImplSwitch.IMPLEMENTATIONS.keySet();
  }

  static class ImplSwitch {
    static final Map<String, HttpClientFactory> IMPLEMENTATIONS;
    static final HttpClientFactory PREFERRED;

    static {
      IMPLEMENTATIONS = new HashMap<>();
      HttpClientFactory preferred = null;
      for (HttpClientFactory s : ServiceLoader.load(HttpClientFactory.class)) {
        if (s.available()) {
          if (preferred == null || preferred.priority() > s.priority()) {
            preferred = s;
          }
          IMPLEMENTATIONS.put(s.name().toLowerCase(ROOT), s);
        }
      }
      PREFERRED = preferred;
    }
  }
}
